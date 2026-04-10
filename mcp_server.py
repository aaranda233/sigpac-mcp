"""MCP Server para SIGPAC: consulta de recintos, agricultores y comparativa de superficies.

Conecta a TecnicosNet y NetOpfh_26 en 192.168.2.36:1433.
Incluye consulta directa a la API SIGPAC para comparar superficies.

PRINCIPIO CRITICO: Nunca devolver datos falsos ni inventados.
Si algo falla, devolver error explícito. Nunca silenciar errores.
"""

import base64
import gzip
import io
import json
import logging
import re
import ssl
import sys
import urllib.request

import PIL.ImageDraw
import pymssql
import staticmaps
from mcp.server.fastmcp import FastMCP
from mcp.types import ImageContent, TextContent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sigpac-mcp")

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

mcp = FastMCP("SIGPAC Explorer", host="0.0.0.0", port=8003)

DB_CONFIG = {
    "server": "192.168.2.36",
    "port": 1433,
    "user": "sa",
    "password": "",
    "charset": "utf8",
}

SIGPAC_API = "https://sigpac-hubcloud.es/servicioconsultassigpac/query"

# Campos obligatorios que debe tener una respuesta SIGPAC válida
SIGPAC_REQUIRED_FIELDS = {"provincia", "municipio", "poligono", "parcela", "recinto", "superficie"}


class SigpacApiError(Exception):
    """Error al consultar la API SIGPAC."""


class SigpacValidationError(Exception):
    """Respuesta SIGPAC inválida o incompleta."""


class DatabaseError(Exception):
    """Error al consultar la base de datos."""


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def _connect(database: str = "TecnicosNet"):
    cfg = DB_CONFIG.copy()
    cfg["database"] = database
    return pymssql.connect(**cfg)


def _query(sql: str, database: str = "TecnicosNet") -> list[dict]:
    try:
        conn = _connect(database)
    except Exception as exc:
        logger.error("Error conectando a BD '%s': %s", database, exc)
        raise DatabaseError(f"No se pudo conectar a la base de datos '{database}': {exc}") from exc
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as exc:
        logger.error("Error ejecutando SQL en '%s': %s", database, exc)
        raise DatabaseError(f"Error ejecutando consulta en '{database}': {exc}") from exc
    finally:
        conn.close()


def _validate_sigpac_response(data: dict, prov, mun, pol, par, rec) -> dict:
    """Valida que la respuesta SIGPAC sea coherente y completa.

    Raises SigpacValidationError si los datos no son fiables.
    """
    missing = SIGPAC_REQUIRED_FIELDS - set(data.keys())
    if missing:
        raise SigpacValidationError(
            f"Respuesta SIGPAC incompleta, faltan campos: {', '.join(sorted(missing))}"
        )

    sup = data.get("superficie")
    if sup is None:
        raise SigpacValidationError("El campo 'superficie' es None en la respuesta SIGPAC")
    if not isinstance(sup, (int, float)):
        raise SigpacValidationError(f"El campo 'superficie' no es numérico: {type(sup).__name__} = {sup!r}")
    if sup < 0:
        raise SigpacValidationError(f"Superficie negativa en SIGPAC: {sup} ha")
    if sup > 100:
        logger.warning("Superficie inusualmente grande: %.4f ha (recinto %s/%s/%s/%s/%s)", sup, prov, mun, pol, par, rec)

    # Verificar que SIGPAC devolvió el recinto correcto (anti-confusión)
    resp_rec = data.get("recinto")
    resp_par = data.get("parcela")
    resp_pol = data.get("poligono")
    resp_mun = data.get("municipio")
    resp_prov = data.get("provincia")

    if resp_prov is not None and int(resp_prov) != int(prov):
        raise SigpacValidationError(
            f"SIGPAC devolvió provincia {resp_prov}, pero se pidió {prov}"
        )
    if resp_mun is not None and int(resp_mun) != int(mun):
        raise SigpacValidationError(
            f"SIGPAC devolvió municipio {resp_mun}, pero se pidió {mun}"
        )
    if resp_pol is not None and int(resp_pol) != int(pol):
        raise SigpacValidationError(
            f"SIGPAC devolvió polígono {resp_pol}, pero se pidió {pol}"
        )
    if resp_par is not None and int(resp_par) != int(par):
        raise SigpacValidationError(
            f"SIGPAC devolvió parcela {resp_par}, pero se pidió {par}"
        )
    if resp_rec is not None and int(resp_rec) != int(rec):
        raise SigpacValidationError(
            f"SIGPAC devolvió recinto {resp_rec}, pero se pidió {rec}"
        )

    return data


def _sigpac_recinfo(prov, mun, pol, par, rec) -> dict:
    """Consulta la API SIGPAC para obtener info oficial de un recinto.

    Returns: dict con datos del recinto.
    Raises: SigpacApiError si no se puede conectar.
            SigpacValidationError si la respuesta es inválida.
    """
    url = f"{SIGPAC_API}/recinfo/{prov}/{mun}/0/0/{pol}/{par}/{rec}.json"
    try:
        req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
        with urllib.request.urlopen(req, timeout=10, context=_SSL_CTX) as resp:
            raw = resp.read()
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
            data = json.loads(raw)
    except urllib.error.HTTPError as exc:
        logger.error("HTTP %s al consultar SIGPAC: %s", exc.code, url)
        raise SigpacApiError(
            f"Error HTTP {exc.code} de la API SIGPAC para recinto {prov}/{mun}/{pol}/{par}/{rec}"
        ) from exc
    except urllib.error.URLError as exc:
        logger.error("Error de red al consultar SIGPAC: %s", exc.reason)
        raise SigpacApiError(
            f"Error de red al consultar SIGPAC para recinto {prov}/{mun}/{pol}/{par}/{rec}: {exc.reason}"
        ) from exc
    except json.JSONDecodeError as exc:
        logger.error("Respuesta SIGPAC no es JSON válido: %s", exc)
        raise SigpacApiError(
            f"Respuesta de SIGPAC no es JSON válido para recinto {prov}/{mun}/{pol}/{par}/{rec}"
        ) from exc
    except Exception as exc:
        logger.error("Error inesperado al consultar SIGPAC: %s", exc)
        raise SigpacApiError(
            f"Error inesperado al consultar SIGPAC para recinto {prov}/{mun}/{pol}/{par}/{rec}: {exc}"
        ) from exc

    # Extraer el registro
    if isinstance(data, list):
        if not data:
            raise SigpacApiError(
                f"SIGPAC devolvió lista vacía para recinto {prov}/{mun}/{pol}/{par}/{rec}"
            )
        record = data[0]
    elif isinstance(data, dict) and data:
        record = data
    else:
        raise SigpacApiError(
            f"Respuesta SIGPAC inesperada (tipo {type(data).__name__}) para recinto {prov}/{mun}/{pol}/{par}/{rec}"
        )

    return _validate_sigpac_response(record, prov, mun, pol, par, rec)


def hectareas_a_m2(hectareas: float) -> int:
    """Convierte hectáreas a metros cuadrados con redondeo correcto."""
    return round(hectareas * 10000)


def calcular_diferencia(sigpac_m2: int, bd_m2: int) -> dict:
    """Calcula diferencia absoluta y porcentual entre SIGPAC y BD."""
    diff_m2 = sigpac_m2 - bd_m2
    diff_pct = round(diff_m2 / bd_m2 * 100, 2) if bd_m2 > 0 else None
    return {"diferencia_m2": diff_m2, "diferencia_pct": diff_pct}


def parse_cod_recinto(cod: str) -> tuple[int, int, int, int, int]:
    """Parsea un código de recinto OPFH '4.66.24.1.3' -> (prov, mun, pol, par, rec).

    Raises ValueError si el formato es inválido.
    """
    parts = cod.strip().split(".")
    if len(parts) != 5:
        raise ValueError(f"Código de recinto inválido '{cod}': debe tener 5 partes separadas por punto (prov.mun.pol.par.rec)")
    try:
        return tuple(int(p) for p in parts)
    except ValueError as exc:
        raise ValueError(f"Código de recinto inválido '{cod}': todas las partes deben ser numéricas") from exc


def _municipio_join() -> str:
    """Genera la cláusula JOIN correcta con la tabla Municipios.

    REC_Provincia (ej: '04') + REC_Municipio (ej: '00079') → código AEMET '04079'.
    MUN_cdMunicipioAemet contiene el código real del municipio (PPMMM).

    Usa MAX(MUN_Id) para evitar duplicados (ej: código 04066 tiene tanto
    CAMPOHERMOSO como NIJAR; el de mayor MUN_Id es el registro más reciente/correcto).
    """
    return (
        "LEFT JOIN ("
        "  SELECT MUN_cdMunicipioAemet AS cod, MAX(MUN_Id) AS mid"
        "  FROM Municipios GROUP BY MUN_cdMunicipioAemet"
        ") mdup ON mdup.cod = "
        "RIGHT('00' + LTRIM(RTRIM(r.REC_Provincia)), 2) + "
        "RIGHT('000' + CAST(TRY_CAST(LTRIM(RTRIM(r.REC_Municipio)) AS INT) AS VARCHAR), 3) "
        "LEFT JOIN Municipios m ON m.MUN_Id = mdup.mid"
    )


def redistribuir_subrecintos(subrecintos: list[dict], sigpac_m2: int, total_bd: int) -> list[dict]:
    """Redistribuye proporcionalmente los subrecintos al total SIGPAC.

    Garantiza que la suma de propuestos == sigpac_m2 exactamente.
    """
    if total_bd <= 0 or not subrecintos:
        return []

    propuesta = []
    for s in subrecintos:
        metros = s.get("metros") or 0
        new_m = round((metros / total_bd) * sigpac_m2)
        propuesta.append({
            "sur_id": s["sur_id"],
            "cod_sub": s.get("cod_sub", ""),
            "actual": metros,
            "propuesto": new_m,
            "diff": new_m - metros,
        })

    # Ajustar residuo al mayor para que la suma sea exacta
    sum_new = sum(p["propuesto"] for p in propuesta)
    remainder = sigpac_m2 - sum_new
    if remainder != 0 and propuesta:
        max_idx = max(range(len(propuesta)), key=lambda i: propuesta[i]["propuesto"])
        propuesta[max_idx]["propuesto"] += remainder
        propuesta[max_idx]["diff"] += remainder

    # Verificación: la suma DEBE ser exacta
    final_sum = sum(p["propuesto"] for p in propuesta)
    assert final_sum == sigpac_m2, (
        f"Error interno: redistribución incorrecta, suma={final_sum} != sigpac={sigpac_m2}"
    )

    return propuesta


# ---------------------------------------------------------------------------
# MAP HELPERS
# ---------------------------------------------------------------------------

_WKT_POLYGON_RE = re.compile(r"POLYGON\s*\(\((.+?)\)\)", re.IGNORECASE)


def parse_wkt_polygon(wkt: str | None) -> list[tuple[float, float]]:
    """Parsea un WKT POLYGON a lista de (lon, lat).

    Raises SigpacValidationError si el WKT es nulo, vacío o no parseable.
    """
    if not wkt or not wkt.strip():
        raise SigpacValidationError("El campo WKT está vacío o ausente")

    # Comprobar MULTIPOLYGON antes que POLYGON (POLYGON matchea dentro de MULTIPOLYGON)
    if "MULTIPOLYGON" in wkt.upper():
        multi_re = re.search(r"MULTIPOLYGON\s*\(\s*\(\s*\((.+?)\)\s*\)", wkt, re.IGNORECASE)
        if not multi_re:
            raise SigpacValidationError(f"No se pudo parsear MULTIPOLYGON: {wkt[:100]}")
        coords_str = multi_re.group(1)
    else:
        m = _WKT_POLYGON_RE.search(wkt)
        if not m:
            raise SigpacValidationError(f"WKT no contiene POLYGON válido: {wkt[:100]}")
        coords_str = m.group(1)

    coords = []
    for pair in coords_str.split(","):
        parts = pair.strip().split()
        if len(parts) < 2:
            continue
        lon, lat = float(parts[0]), float(parts[1])
        coords.append((lon, lat))

    if len(coords) < 3:
        raise SigpacValidationError(f"Polígono con menos de 3 vértices ({len(coords)})")

    return coords


# Monkey-patch: Pillow 10+ eliminó ImageDraw.textsize, py-staticmaps lo usa para attribution
if not hasattr(PIL.ImageDraw.ImageDraw, "textsize"):
    def _textsize(self, text, font=None, **kwargs):
        left, top, right, bottom = self.textbbox((0, 0), text, font=font, **kwargs)
        return right - left, bottom - top
    PIL.ImageDraw.ImageDraw.textsize = _textsize

# Tile providers sin attribution (evita problemas de renderizado de texto)
_ARCGIS_TILE_PROVIDER = staticmaps.TileProvider(
    name="arcgis_world_imagery",
    url_pattern="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/$z/$y/$x",
    max_zoom=19,
)

_OSM_TILE_PROVIDER = staticmaps.TileProvider(
    name="osm",
    url_pattern="https://$s.tile.openstreetmap.org/$z/$x/$y.png",
    shards=["a", "b", "c"],
    max_zoom=19,
)


def render_recinto_map(
    coords: list[tuple[float, float]],
    width: int = 600,
    height: int = 400,
) -> bytes:
    """Renderiza un mapa PNG con el polígono del recinto.

    Args:
        coords: Lista de (lon, lat) del polígono.
        width: Ancho de la imagen en píxeles.
        height: Alto de la imagen en píxeles.

    Returns: bytes PNG de la imagen.
    """
    latlngs = [staticmaps.create_latlng(lat, lon) for lon, lat in coords]

    area = staticmaps.Area(
        latlngs,
        fill_color=staticmaps.parse_color("#ff00ff1a"),
        color=staticmaps.parse_color("#ff00ff"),
        width=3,
    )

    for provider in [_ARCGIS_TILE_PROVIDER, _OSM_TILE_PROVIDER]:
        try:
            context = staticmaps.Context()
            context.set_tile_provider(provider)
            context.add_object(area)
            # Zoom automático + boost para que el recinto se vea grande
            _center, auto_zoom = context.determine_center_zoom(width, height)
            context.set_zoom(min(auto_zoom + 2, 18))
            image = context.render_pillow(width, height)
            break
        except Exception as exc:
            logger.warning("Tiles %s fallaron: %s", provider.name(), exc)
    else:
        # Último recurso: fondo blanco con polígono
        from PIL import Image as PILImage, ImageDraw
        image = PILImage.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        min_lon, max_lon = min(lons), max(lons)
        min_lat, max_lat = min(lats), max(lats)
        range_lon = max_lon - min_lon or 0.001
        range_lat = max_lat - min_lat or 0.001
        margin = 20
        w, h = width - 2 * margin, height - 2 * margin
        scaled = [
            (margin + (lon - min_lon) / range_lon * w,
             margin + (1 - (lat - min_lat) / range_lat) * h)
            for lon, lat in coords
        ]
        draw.polygon(scaled, outline="#ff00ff")

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# TOOLS
# ---------------------------------------------------------------------------

@mcp.tool()
def buscar_agricultores(nombre: str = "", municipio: str = "", solo_activos: bool = True) -> list[dict]:
    """Busca agricultores en la BD por nombre o municipio.

    Args:
        nombre: Texto parcial del nombre del agricultor (ej: 'VARGAS', 'BIO ALUMA').
        municipio: Nombre del municipio para filtrar (ej: 'BERJA', 'ADRA').
        solo_activos: Si True, solo agricultores activos (por defecto True).
    """
    where = ["a.AGR_Nombre != ''"]
    if solo_activos:
        where.append("EXISTS (SELECT 1 FROM Cultivos c WHERE c.CUL_IdAgriCultivo = a.AGR_Idagricultor AND c.CUL_FechaFinalizaReal = '1900-01-01')")
    if nombre:
        where.append(f"LOWER(a.AGR_Nombre) LIKE '%{nombre.lower().replace(chr(39), '')}%'")
    if municipio:
        where.append(f"LOWER(m.MUN_Nombre) LIKE '%{municipio.lower().replace(chr(39), '')}%'")

    sql = f"""
        SELECT TOP 50
            a.AGR_Idagricultor AS id, a.AGR_Nombre AS nombre, a.AGR_Nif AS nif,
            a.AGR_Poblacion AS poblacion, a.AGR_Provincia AS provincia,
            COUNT(DISTINCT r.REC_Id) AS num_recintos
        FROM Agricultores a
        LEFT JOIN Fincas f ON f.FIN_IdAgricultor = a.AGR_Idagricultor
        LEFT JOIN RecintosSigpac r ON r.REC_IdFinca = f.FIN_IdFinca
            AND r.REC_FechaBaja = '1900-01-01'
        {_municipio_join()}
        WHERE {' AND '.join(where)}
        GROUP BY a.AGR_Idagricultor, a.AGR_Nombre, a.AGR_Nif, a.AGR_Poblacion, a.AGR_Provincia
        ORDER BY a.AGR_Nombre
    """
    try:
        rows = _query(sql)
    except DatabaseError as exc:
        return [{"error": f"Error consultando agricultores: {exc}"}]

    for r in rows:
        for k, v in r.items():
            if isinstance(v, str):
                r[k] = v.strip()
    return rows


@mcp.tool()
def recintos_agricultor(agricultor_id: int) -> list[dict]:
    """Lista todos los recintos de un agricultor con sus datos SIGPAC.

    Args:
        agricultor_id: ID del agricultor (AGR_Idagricultor).
    """
    sql = f"""
        SELECT TOP 200
            r.REC_Id AS id,
            r.REC_Provincia AS provincia, r.REC_Municipio AS municipio,
            r.REC_Poligono AS poligono, r.REC_Parcela AS parcela, r.REC_Recinto AS recinto,
            r.REC_SuperficieSigPac AS superficie_bd,
            m.MUN_Nombre AS municipio_nombre,
            f.FIN_Nombre AS finca_nombre
        FROM RecintosSigpac r
        LEFT JOIN Fincas f ON r.REC_IdFinca = f.FIN_IdFinca
        {_municipio_join()}
        WHERE f.FIN_IdAgricultor = {int(agricultor_id)}
          AND r.REC_FechaBaja = '1900-01-01'
        ORDER BY r.REC_Municipio, r.REC_Poligono, r.REC_Parcela, r.REC_Recinto
    """
    try:
        rows = _query(sql)
    except DatabaseError as exc:
        return [{"error": f"Error consultando recintos del agricultor {agricultor_id}: {exc}"}]

    for r in rows:
        for k, v in r.items():
            if isinstance(v, str):
                r[k] = v.strip()
    return rows


@mcp.tool()
def comparar_recinto(provincia: int, municipio: int, poligono: int, parcela: int, recinto: int) -> dict:
    """Compara la superficie de un recinto en la BD con la superficie oficial de SIGPAC.

    IMPORTANTE: Si no se puede obtener la superficie de SIGPAC, devuelve error explícito.
    Nunca devuelve datos inventados.

    Args:
        provincia: Codigo de provincia (ej: 4 para Almeria).
        municipio: Codigo de municipio.
        poligono: Numero de poligono.
        parcela: Numero de parcela.
        recinto: Numero de recinto.
    """
    ref = f"{provincia}/{municipio}/{poligono}/{parcela}/{recinto}"

    # BD
    try:
        sql = f"""
            SELECT TOP 1
                r.REC_Id, r.REC_SuperficieSigPac AS superficie_bd,
                a.AGR_Nombre AS agricultor, a.AGR_Nif AS nif
            FROM RecintosSigpac r
            LEFT JOIN Fincas f2 ON r.REC_IdFinca = f2.FIN_IdFinca
            LEFT JOIN Agricultores a ON f2.FIN_IdAgricultor = a.AGR_Idagricultor
            WHERE TRY_CAST(r.REC_Municipio AS INT) = {int(municipio)}
              AND TRY_CAST(r.REC_Poligono AS INT) = {int(poligono)}
              AND TRY_CAST(r.REC_Parcela AS INT) = {int(parcela)}
              AND TRY_CAST(r.REC_Recinto AS INT) = {int(recinto)}
              AND r.REC_FechaBaja = '1900-01-01'
        """
        bd_rows = _query(sql)
    except DatabaseError as exc:
        return {"error": f"Error consultando BD para recinto {ref}: {exc}"}

    bd = bd_rows[0] if bd_rows else None

    # SIGPAC API
    try:
        sigpac = _sigpac_recinfo(provincia, municipio, poligono, parcela, recinto)
    except (SigpacApiError, SigpacValidationError) as exc:
        return {
            "error": f"No se pudo obtener datos fiables de SIGPAC para recinto {ref}: {exc}",
            "recinto": f"{provincia}-{municipio}-{poligono}-{parcela}-{recinto}",
            "bd_m2": round(float(bd["superficie_bd"])) if bd and bd["superficie_bd"] is not None else None,
            "sigpac_m2": None,
            "datos_sigpac_fiables": False,
        }

    sigpac_m2 = hectareas_a_m2(sigpac["superficie"])
    bd_m2 = round(float(bd["superficie_bd"])) if bd and bd["superficie_bd"] is not None else None

    result = {
        "recinto": f"{provincia}-{municipio}-{poligono}-{parcela}-{recinto}",
        "bd_m2": bd_m2,
        "sigpac_m2": sigpac_m2,
        "datos_sigpac_fiables": True,
        "agricultor": bd["agricultor"].strip() if bd and bd.get("agricultor") else None,
        "nif": bd["nif"].strip() if bd and bd.get("nif") else None,
        "sigpac_uso": sigpac.get("uso_sigpac"),
        "sigpac_coef_regadio": sigpac.get("coef_regadio"),
        "sigpac_pendiente": sigpac.get("pendiente_media"),
        "sigpac_region": sigpac.get("region"),
    }

    if sigpac_m2 is not None and bd_m2 is not None:
        diff = calcular_diferencia(sigpac_m2, bd_m2)
        result["diferencia_m2"] = diff["diferencia_m2"]
        result["diferencia_pct"] = diff["diferencia_pct"]
    else:
        result["diferencia_m2"] = None
        result["diferencia_pct"] = None

    return result


@mcp.tool()
def listar_diferencias_opfh(min_pct: float = 0, max_results: int = 50) -> list[dict]:
    """Lista recintos de OPFH (agricultores activos) cuya superficie BD difiere de SIGPAC.

    Consulta NetOpfh_26 y compara con la API SIGPAC oficial. Solo devuelve los que tienen diferencia.
    ATENCION: puede tardar porque consulta la API SIGPAC para cada recinto.
    Los recintos cuya consulta SIGPAC falle se reportan con error, no se omiten.

    Args:
        min_pct: Porcentaje minimo de diferencia para incluir (ej: 5 para >5%). Por defecto 0.
        max_results: Maximo de resultados a devolver (por defecto 50).
    """
    sql = """
        SELECT DISTINCT TOP 200
            r.REC_CodRecinto AS cod, r.REC_IdProvincia AS prov, r.REC_IdMunicipio AS mun,
            r.REC_IdPoligono AS pol, r.REC_IdParcela AS par, r.REC_IdRecinto AS rec,
            r.REC_Metros AS metros_bd
        FROM NetOpfh_26.dbo.Recintos r
        INNER JOIN NetOpfh_26.dbo.SubRecintos s ON r.REC_CodRecinto = s.SUR_CodRecinto
        INNER JOIN (SELECT DISTINCT CUL_IdAgriCultivo AS AGR_Idagricultor FROM Cultivos WHERE CUL_FechaFinalizaReal = '1900-01-01') aa ON s.SUR_IdAgricultor = aa.AGR_Idagricultor
        ORDER BY r.REC_CodRecinto
    """
    try:
        recintos = _query(sql)
    except DatabaseError as exc:
        return [{"error": f"Error consultando recintos OPFH: {exc}"}]

    results = []
    errores = []
    for r in recintos:
        if len(results) >= max_results:
            break
        try:
            sigpac = _sigpac_recinfo(r["prov"], r["mun"], r["pol"], r["par"], r["rec"])
        except (SigpacApiError, SigpacValidationError) as exc:
            errores.append({"cod": r["cod"], "error": str(exc)})
            continue

        sigpac_m2 = hectareas_a_m2(sigpac["superficie"])
        bd_m2 = r["metros_bd"] or 0
        if sigpac_m2 == bd_m2:
            continue
        diff = sigpac_m2 - bd_m2
        pct = round(diff / bd_m2 * 100, 2) if bd_m2 > 0 else None
        if pct is not None and abs(pct) < min_pct:
            continue
        results.append({
            "cod": r["cod"],
            "bd_m2": bd_m2,
            "sigpac_m2": sigpac_m2,
            "diff_m2": diff,
            "diff_pct": pct,
        })

    if errores:
        results.append({
            "aviso": f"{len(errores)} recinto(s) no pudieron consultarse en SIGPAC",
            "errores": errores[:10],
        })

    return results


@mcp.tool()
def subrecintos_recinto(cod_recinto: str) -> dict:
    """Obtiene los subrecintos de un recinto en NetOpfh_26 con detalle de agricultor.

    Args:
        cod_recinto: Codigo del recinto en formato OPFH (ej: '4.66.24.1.3').
    """
    cod = cod_recinto.replace("'", "")

    try:
        prov, mun, pol, par, rec = parse_cod_recinto(cod)
    except ValueError as exc:
        return {"error": str(exc)}

    sql = f"""
        SELECT
            r.REC_CodRecinto AS cod, r.REC_Metros AS rec_metros,
            s.SUR_Id, s.SUR_CodSubRecinto AS cod_sub, s.SUR_SubRecinto AS sub,
            s.SUR_Metros AS metros, s.SUR_IdAgricultor AS agr_id,
            a.AGR_Nombre AS agricultor
        FROM NetOpfh_26.dbo.Recintos r
        INNER JOIN NetOpfh_26.dbo.SubRecintos s ON r.REC_CodRecinto = s.SUR_CodRecinto
        LEFT JOIN Agricultores a ON s.SUR_IdAgricultor = a.AGR_Idagricultor
        WHERE r.REC_CodRecinto = '{cod}'
        ORDER BY s.SUR_Id
    """
    try:
        rows = _query(sql)
    except DatabaseError as exc:
        return {"error": f"Error consultando subrecintos de '{cod}': {exc}"}

    if not rows:
        return {"error": f"Recinto '{cod}' no encontrado en la base de datos"}

    rec_metros = rows[0]["rec_metros"]

    # Consultar SIGPAC
    sigpac_m2 = None
    sigpac_error = None
    try:
        sigpac = _sigpac_recinfo(prov, mun, pol, par, rec)
        sigpac_m2 = hectareas_a_m2(sigpac["superficie"])
    except (SigpacApiError, SigpacValidationError) as exc:
        sigpac_error = str(exc)

    subrecintos = []
    for r in rows:
        subrecintos.append({
            "sur_id": r["SUR_Id"],
            "cod_sub": (r["cod_sub"] or "").strip(),
            "metros": r["metros"],
            "agricultor_id": r["agr_id"],
            "agricultor": (r["agricultor"] or "").strip(),
        })

    result = {
        "cod": cod,
        "rec_metros_bd": rec_metros,
        "sigpac_m2": sigpac_m2,
        "datos_sigpac_fiables": sigpac_error is None,
        "subrecintos": subrecintos,
        "sum_subrecintos": sum(s["metros"] or 0 for s in subrecintos),
    }

    if sigpac_error:
        result["error_sigpac"] = sigpac_error
        result["diferencia_m2"] = None
    else:
        result["diferencia_m2"] = (sigpac_m2 - rec_metros) if sigpac_m2 is not None and rec_metros is not None else None

    # Propuesta de redistribucion solo si tenemos datos SIGPAC fiables
    if sigpac_m2 is not None and rec_metros and sigpac_m2 != rec_metros and sigpac_error is None:
        result["propuesta_redistribucion"] = redistribuir_subrecintos(subrecintos, sigpac_m2, rec_metros)

    return result


@mcp.tool()
def consultar_sigpac(provincia: int, municipio: int, poligono: int, parcela: int, recinto: int) -> dict:
    """Consulta la API oficial de SIGPAC para obtener datos de un recinto.

    IMPORTANTE: Si la consulta falla o los datos no son fiables, devuelve error explícito.

    Args:
        provincia: Codigo de provincia (ej: 4 para Almeria).
        municipio: Codigo de municipio.
        poligono: Numero de poligono.
        parcela: Numero de parcela.
        recinto: Numero de recinto.
    """
    try:
        info = _sigpac_recinfo(provincia, municipio, poligono, parcela, recinto)
    except (SigpacApiError, SigpacValidationError) as exc:
        return {
            "error": str(exc),
            "recinto_solicitado": f"{provincia}/{municipio}/{poligono}/{parcela}/{recinto}",
            "datos_fiables": False,
        }

    # Clean up response - quitar WKT (muy largo) pero mantener todo lo demás
    result = {"datos_fiables": True}
    for k, v in info.items():
        if v is not None and k != "wkt":
            result[k] = v
    return result


@mcp.tool()
def run_select(sql: str, database: str = "TecnicosNet") -> list[dict]:
    """Ejecuta una consulta SELECT de solo lectura (max 200 filas).

    SOLO se permiten SELECT. Cualquier INSERT, UPDATE, DELETE, etc. sera rechazado.
    Bases de datos permitidas: TecnicosNet, NetOpfh_26.

    Args:
        sql: Consulta SQL (debe empezar con SELECT o WITH).
        database: Base de datos (TecnicosNet o NetOpfh_26).
    """
    allowed = {"tecnicosnet", "netopfh_26"}
    if database.lower() not in allowed:
        return [{"error": f"Base de datos '{database}' no permitida. Solo: TecnicosNet, NetOpfh_26."}]

    sql_stripped = sql.strip()
    if not re.match(r"^(SELECT|WITH)\b", sql_stripped, re.IGNORECASE):
        return [{"error": "La consulta debe empezar con SELECT."}]

    forbidden = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|MERGE|GRANT|REVOKE|DENY|"
        r"BACKUP|RESTORE|SHUTDOWN|DBCC|OPENROWSET|OPENDATASOURCE|XP_|SP_)\b",
        re.IGNORECASE,
    )
    match = forbidden.search(sql_stripped)
    if match:
        return [{"error": f"Operacion prohibida: {match.group()}. Solo SELECT."}]

    if ";" in sql_stripped:
        return [{"error": "No se permiten multiples sentencias (;)."}]

    if not re.search(r"\bTOP\b", sql_stripped, re.IGNORECASE):
        sql_stripped = re.sub(r"(?i)^SELECT", "SELECT TOP 200", sql_stripped, count=1)

    try:
        rows = _query(sql_stripped, database)
        for row in rows:
            for k, v in row.items():
                row[k] = str(v)[:500] if v is not None else None
        return rows
    except DatabaseError as exc:
        return [{"error": str(exc)}]
    except Exception as exc:
        return [{"error": f"Error inesperado: {exc}"}]


@mcp.tool()
def mapa_recinto(
    provincia: int,
    municipio: int,
    poligono: int,
    parcela: int,
    recinto: int,
    ancho: int = 600,
    alto: int = 400,
) -> list[TextContent | ImageContent]:
    """Genera una imagen de mapa con el recinto SIGPAC dibujado sobre imagen satélite.

    Devuelve la imagen PNG en base64 junto con metadatos del recinto.
    Útil para visualizar la ubicación y forma exacta de un recinto.

    Args:
        provincia: Codigo de provincia (ej: 4 para Almeria).
        municipio: Codigo de municipio.
        poligono: Numero de poligono.
        parcela: Numero de parcela.
        recinto: Numero de recinto.
        ancho: Ancho de la imagen en píxeles (200-1200, por defecto 600).
        alto: Alto de la imagen en píxeles (200-1200, por defecto 400).
    """
    ref = f"{provincia}/{municipio}/{poligono}/{parcela}/{recinto}"

    # Validar tamaño
    ancho = max(200, min(1200, ancho))
    alto = max(200, min(1200, alto))

    # Obtener datos del recinto incluyendo WKT
    try:
        info = _sigpac_recinfo(provincia, municipio, poligono, parcela, recinto)
    except (SigpacApiError, SigpacValidationError) as exc:
        return [TextContent(type="text", text=f"Error obteniendo datos SIGPAC para {ref}: {exc}")]

    wkt = info.get("wkt")
    try:
        coords = parse_wkt_polygon(wkt)
    except SigpacValidationError as exc:
        return [TextContent(type="text", text=f"Error parseando geometría de {ref}: {exc}")]

    # Renderizar mapa
    try:
        png_bytes = render_recinto_map(coords, ancho, alto)
    except Exception as exc:
        logger.error("Error renderizando mapa de %s: %s", ref, exc)
        return [TextContent(type="text", text=f"Error generando imagen del mapa para {ref}: {exc}")]

    # Metadatos
    superficie_ha = info.get("superficie", "?")
    uso = info.get("uso_sigpac", "?")
    coef_reg = info.get("coef_regadio", "?")
    region = info.get("region", "?")

    metadata = (
        f"Recinto {ref}\n"
        f"Superficie: {superficie_ha} ha\n"
        f"Uso SIGPAC: {uso}\n"
        f"Coef. regadío: {coef_reg}\n"
        f"Región: {region}"
    )

    b64 = base64.b64encode(png_bytes).decode("utf-8")

    return [
        TextContent(type="text", text=metadata),
        ImageContent(type="image", data=b64, mimeType="image/png"),
    ]


# ---------------------------------------------------------------------------
# TÉCNICOS
# ---------------------------------------------------------------------------

_LATEST_TECNICO_CTE = """
WITH LatestTecnico AS (
    SELECT f.FIN_IdAgricultor, c.CUL_IdTecnico,
           ROW_NUMBER() OVER (PARTITION BY f.FIN_IdAgricultor ORDER BY c.CUL_IdTemporada DESC, c.CUL_IdCultivo DESC) AS rn
    FROM Cultivos c INNER JOIN Fincas f ON c.CUL_IdFinca = f.FIN_IdFinca WHERE c.CUL_IdTecnico > 0
)
"""


@mcp.tool()
def listar_tecnicos() -> list[dict]:
    """Lista todos los técnicos con el número de agricultores asignados (última temporada)."""
    return _query(f"""
        {_LATEST_TECNICO_CTE}
        SELECT t.TEC_IdTecnico AS id, LTRIM(RTRIM(t.TEC_Nombre)) AS nombre, t.TEC_Activo AS activo,
               COUNT(DISTINCT lt.FIN_IdAgricultor) AS agricultores
        FROM LatestTecnico lt
        INNER JOIN Tecnicos t ON lt.CUL_IdTecnico = TRY_CAST(t.TEC_IdTecnico AS INT)
        INNER JOIN Agricultores a ON lt.FIN_IdAgricultor = a.AGR_Idagricultor AND a.AGR_Nombre != ''
        WHERE lt.rn = 1
        GROUP BY t.TEC_IdTecnico, t.TEC_Nombre, t.TEC_Activo
        HAVING COUNT(DISTINCT lt.FIN_IdAgricultor) > 0
        ORDER BY t.TEC_Nombre
    """)


@mcp.tool()
def agricultores_tecnico(tecnico_id: int) -> list[dict]:
    """Devuelve los agricultores asignados a un técnico (por última temporada), con NIF y nº de recintos."""
    rows = _query(f"""
        {_LATEST_TECNICO_CTE}
        SELECT a.AGR_Idagricultor AS id, LTRIM(RTRIM(a.AGR_Nombre)) AS nombre, LTRIM(RTRIM(a.AGR_Nif)) AS nif,
               COUNT(DISTINCT r.REC_Id) AS recintos
        FROM LatestTecnico lt
        INNER JOIN Agricultores a ON lt.FIN_IdAgricultor = a.AGR_Idagricultor AND a.AGR_Nombre != ''
        LEFT JOIN Fincas f2 ON f2.FIN_IdAgricultor = a.AGR_Idagricultor
        LEFT JOIN RecintosSigpac r ON r.REC_IdFinca = f2.FIN_IdFinca AND r.REC_FechaBaja = '1900-01-01' AND r.REC_Provincia != ''
        WHERE lt.rn = 1 AND lt.CUL_IdTecnico = {int(tecnico_id)}
        GROUP BY a.AGR_Idagricultor, a.AGR_Nombre, a.AGR_Nif
        ORDER BY a.AGR_Nombre
    """)
    return rows


@mcp.tool()
def detalle_tecnicos(solo_activos: bool = True) -> list[dict]:
    """Dashboard de técnicos: agricultores, zonas (provincia/municipio), BIO/CONV, superficie.

    Args:
        solo_activos: Si True, solo muestra agricultores con cultivos activos (no finalizados).
    """
    activo_join = (
        "INNER JOIN (SELECT DISTINCT CUL_IdAgriCultivo AS AGR_Idagricultor FROM Cultivos "
        "WHERE CUL_FechaFinalizaReal = '1900-01-01') aa ON a.AGR_Idagricultor = aa.AGR_Idagricultor"
        if solo_activos else ""
    )
    rows = _query(f"""
        {_LATEST_TECNICO_CTE}
        SELECT CAST(t.TEC_IdTecnico AS INT) AS tecId, LTRIM(RTRIM(t.TEC_Nombre)) AS tecNombre, t.TEC_Activo AS tecActivo,
               a.AGR_Idagricultor AS agrId, LTRIM(RTRIM(a.AGR_Nombre)) AS agrNombre, LTRIM(RTRIM(a.AGR_Nif)) AS agrNif,
               UPPER(LTRIM(RTRIM(f2.FIN_Provincia))) AS provincia, LTRIM(RTRIM(f2.FIN_Municipio)) AS municipio,
               COUNT(DISTINCT r2.REC_Id) AS recintos, SUM(r2.REC_SuperficieSigPac) AS superficie
        FROM LatestTecnico lt
        INNER JOIN Tecnicos t ON lt.CUL_IdTecnico = TRY_CAST(t.TEC_IdTecnico AS INT)
        INNER JOIN Agricultores a ON lt.FIN_IdAgricultor = a.AGR_Idagricultor AND a.AGR_Nombre != ''
        {activo_join}
        LEFT JOIN Fincas f2 ON f2.FIN_IdAgricultor = a.AGR_Idagricultor
        LEFT JOIN RecintosSigpac r2 ON r2.REC_IdFinca = f2.FIN_IdFinca AND r2.REC_FechaBaja = '1900-01-01' AND r2.REC_Provincia != ''
        WHERE lt.rn = 1
        GROUP BY t.TEC_IdTecnico, t.TEC_Nombre, t.TEC_Activo,
                 a.AGR_Idagricultor, a.AGR_Nombre, a.AGR_Nif,
                 UPPER(LTRIM(RTRIM(f2.FIN_Provincia))), LTRIM(RTRIM(f2.FIN_Municipio))
        ORDER BY t.TEC_Nombre, a.AGR_Nombre
    """)
    # Aggregate into structured per-tecnico
    norm = {"ALMERÍA": "ALMERIA", "JAÉN": "JAEN", "MÁLAGA": "MALAGA"}
    tecs: dict = {}
    for r in rows:
        tid = r["tecId"]
        if tid not in tecs:
            tecs[tid] = {"id": tid, "nombre": r["tecNombre"].strip(), "activo": r["tecActivo"] == "S", "agricultores": {}, "zonas": {}}
        t = tecs[tid]
        aid = r["agrId"]
        if aid not in t["agricultores"]:
            t["agricultores"][aid] = {"id": aid, "nombre": r["agrNombre"], "nif": r["agrNif"], "recintos": 0, "superficie": 0.0, "municipios": []}
        ag = t["agricultores"][aid]
        ag["recintos"] += r["recintos"] or 0
        ag["superficie"] += float(r["superficie"] or 0)
        if r["municipio"]:
            ag["municipios"].append(r["municipio"])
        prov_raw = (r["provincia"] or "").strip()
        prov = norm.get(prov_raw, prov_raw)
        mun = r["municipio"] or ""
        if prov and mun and (r["recintos"] or 0) > 0:
            if prov not in t["zonas"]:
                t["zonas"][prov] = {}
            if mun not in t["zonas"][prov]:
                t["zonas"][prov][mun] = {"agricultores": 0, "recintos": 0}
            t["zonas"][prov][mun]["agricultores"] += 1
            t["zonas"][prov][mun]["recintos"] += r["recintos"] or 0
    result = []
    for t in tecs.values():
        agris = [
            {**a, "municipios": list(set(a["municipios"])), "superficie": round(a["superficie"], 2)}
            for a in t["agricultores"].values() if a["recintos"] > 0
        ]
        result.append({
            "id": t["id"], "nombre": t["nombre"], "activo": t["activo"],
            "totalAgricultores": len(agris),
            "totalRecintos": sum(a["recintos"] for a in agris),
            "totalSuperficie": round(sum(a["superficie"] for a in agris), 2),
            "provincias": list(t["zonas"].keys()),
            "zonas": t["zonas"],
            "agricultores": agris,
        })
    return result


# ---------------------------------------------------------------------------
# INCIDENCIAS
# ---------------------------------------------------------------------------


@mcp.tool()
def incidencias(temporada_id: int = 0, tecnico_id: int = 0) -> dict:
    """Detecta incidencias: cultivos sin cerrar, recintos compartidos y recintos huérfanos.

    Args:
        temporada_id: Filtrar cultivos sin cerrar por ID de temporada (0 = todas).
        tecnico_id: Filtrar cultivos sin cerrar por ID de técnico (0 = todos).
    """
    # 1. Recintos compartidos
    compartidos = _query("""
        SELECT
            TRY_CAST(r.REC_Provincia AS INT) AS prov, TRY_CAST(r.REC_Municipio AS INT) AS mun,
            TRY_CAST(r.REC_Poligono AS INT) AS pol, TRY_CAST(r.REC_Parcela AS INT) AS par, TRY_CAST(r.REC_Recinto AS INT) AS rec,
            f.FIN_IdAgricultor AS agrId, LTRIM(RTRIM(a.AGR_Nombre)) AS agrNombre, LTRIM(RTRIM(a.AGR_Nif)) AS agrNif,
            LTRIM(RTRIM(f.FIN_Municipio)) AS municipio
        FROM RecintosSigpac r
        INNER JOIN Fincas f ON r.REC_IdFinca = f.FIN_IdFinca
        INNER JOIN Agricultores a ON f.FIN_IdAgricultor = a.AGR_Idagricultor AND a.AGR_Nombre != ''
        WHERE r.REC_FechaBaja = '1900-01-01' AND r.REC_Provincia != ''
        AND EXISTS (
            SELECT 1 FROM RecintosSigpac r2
            INNER JOIN Fincas f2 ON r2.REC_IdFinca = f2.FIN_IdFinca
            WHERE r2.REC_FechaBaja = '1900-01-01'
              AND TRY_CAST(r2.REC_Provincia AS INT) = TRY_CAST(r.REC_Provincia AS INT)
              AND TRY_CAST(r2.REC_Municipio AS INT) = TRY_CAST(r.REC_Municipio AS INT)
              AND TRY_CAST(r2.REC_Poligono AS INT) = TRY_CAST(r.REC_Poligono AS INT)
              AND TRY_CAST(r2.REC_Parcela AS INT) = TRY_CAST(r.REC_Parcela AS INT)
              AND TRY_CAST(r2.REC_Recinto AS INT) = TRY_CAST(r.REC_Recinto AS INT)
              AND f2.FIN_IdAgricultor != f.FIN_IdAgricultor
        )
        ORDER BY TRY_CAST(r.REC_Provincia AS INT), TRY_CAST(r.REC_Municipio AS INT)
    """)
    rec_map: dict = {}
    for r in compartidos:
        key = f"{r['prov']}-{r['mun']}-{r['pol']}-{r['par']}-{r['rec']}"
        if key not in rec_map:
            rec_map[key] = {"key": key, "municipio": r["municipio"] or "", "agricultores": []}
        if not any(a["id"] == r["agrId"] for a in rec_map[key]["agricultores"]):
            rec_map[key]["agricultores"].append({"id": r["agrId"], "nombre": r["agrNombre"], "nif": r["agrNif"]})
    rec_compartidos = list(rec_map.values())

    # 2. Recintos huérfanos
    huerfanos = _query("""
        SELECT REC_Id AS id, REC_Provincia AS prov, REC_Municipio AS mun,
               REC_Poligono AS pol, REC_Parcela AS par, REC_Recinto AS rec, REC_IdFinca AS fincaId
        FROM RecintosSigpac
        WHERE REC_FechaBaja = '1900-01-01'
          AND (REC_IdFinca IS NULL OR REC_IdFinca = 0 OR NOT EXISTS (SELECT 1 FROM Fincas f WHERE f.FIN_IdFinca = REC_IdFinca))
    """)

    # 3. Cultivos sin cerrar
    where_extra = ""
    if temporada_id:
        where_extra += f" AND c.CUL_IdTemporada = {int(temporada_id)}"
    if tecnico_id:
        where_extra += f" AND c.CUL_IdTecnico = {int(tecnico_id)}"
    cultivos = _query(f"""
        SELECT c.CUL_IdCultivo AS id, c.CUL_IdTemporada AS tempId, LTRIM(RTRIM(tmp.TEM_Nombre)) AS temporada,
               a.AGR_Idagricultor AS agrId, LTRIM(RTRIM(a.AGR_Nombre)) AS agricultor,
               f.FIN_IdFinca AS fincaId, LTRIM(RTRIM(f.FIN_Nombre)) AS finca,
               CAST(t.TEC_IdTecnico AS INT) AS tecId, LTRIM(RTRIM(t.TEC_Nombre)) AS tecnico,
               LTRIM(RTRIM(t.TEC_Telefono)) AS tecTelefono, LTRIM(RTRIM(t.TEC_Email)) AS tecEmail,
               LTRIM(RTRIM(g.GEN_Nombre)) AS genero
        FROM Cultivos c
        INNER JOIN Temporadas tmp ON c.CUL_IdTemporada = tmp.TEM_IdTemporada
        INNER JOIN Fincas f ON c.CUL_IdFinca = f.FIN_IdFinca
        INNER JOIN Agricultores a ON f.FIN_IdAgricultor = a.AGR_Idagricultor
        LEFT JOIN Tecnicos t ON c.CUL_IdTecnico = TRY_CAST(t.TEC_IdTecnico AS INT)
        LEFT JOIN Generos g ON c.CUL_IdGenero = g.GEN_IdGenero
        WHERE c.CUL_FechaFinalizaReal = '1900-01-01'{where_extra}
        ORDER BY c.CUL_IdTemporada DESC, a.AGR_Nombre
    """)

    # Temporadas with open cultivos
    temp_map: dict = {}
    for c in cultivos:
        tid = c["tempId"]
        if tid not in temp_map:
            temp_map[tid] = {"id": tid, "nombre": c["temporada"], "count": 0}
        temp_map[tid]["count"] += 1
    temporadas = sorted(temp_map.values(), key=lambda x: x["id"], reverse=True)

    return {
        "resumen": {
            "recintosCompartidos": len(rec_compartidos),
            "recintosHuerfanos": len(huerfanos),
            "cultivosSinCerrar": len(cultivos),
            "total": len(rec_compartidos) + len(huerfanos) + len(cultivos),
        },
        "recCompartidos": rec_compartidos,
        "recHuerfanos": huerfanos,
        "cultivosSinCerrar": cultivos,
        "temporadas": temporadas,
    }


@mcp.tool()
def zona_influencia_tecnicos(provincias: list[int]) -> dict:
    """Datos de zona de influencia por técnico: recintos con técnico asignado y tipo BIO/CONV.

    Args:
        provincias: Lista de códigos de provincia (4=Almería, 18=Granada, 23=Jaén, 29=Málaga, 30=Murcia).
    """
    code_to_name = {4: "ALMERIA", 18: "GRANADA", 23: "JAEN", 29: "MALAGA", 30: "MURCIA"}
    accent = {"ALMERIA": "ALMERÍA", "JAEN": "JAÉN", "MALAGA": "MÁLAGA"}
    prov_names = []
    for c in provincias:
        name = code_to_name.get(int(c), "")
        if name:
            prov_names.append(f"'{name}'")
            if name in accent:
                prov_names.append(f"'{accent[name]}'")
    if not prov_names:
        return {"error": "provincias requeridas"}
    prov_in = ", ".join(prov_names)

    rows = _query(f"""
        {_LATEST_TECNICO_CTE}
        SELECT
            TRY_CAST(r.REC_Provincia AS INT) AS prov, TRY_CAST(r.REC_Municipio AS INT) AS mun,
            TRY_CAST(r.REC_Poligono AS INT) AS pol, TRY_CAST(r.REC_Parcela AS INT) AS par,
            TRY_CAST(r.REC_Recinto AS INT) AS rec,
            LTRIM(RTRIM(f.FIN_Municipio)) AS munNombre,
            a.AGR_Idagricultor AS agrId, LTRIM(RTRIM(a.AGR_Nombre)) AS agrNombre,
            CAST(t.TEC_IdTecnico AS INT) AS tecId, LTRIM(RTRIM(t.TEC_Nombre)) AS tecNombre
        FROM RecintosSigpac r
        LEFT JOIN Fincas f ON r.REC_IdFinca = f.FIN_IdFinca
        INNER JOIN Agricultores a ON f.FIN_IdAgricultor = a.AGR_Idagricultor AND a.AGR_Nombre != ''
        INNER JOIN (SELECT DISTINCT CUL_IdAgriCultivo AS AGR_Idagricultor FROM Cultivos WHERE CUL_FechaFinalizaReal = '1900-01-01') aa ON a.AGR_Idagricultor = aa.AGR_Idagricultor
        LEFT JOIN LatestTecnico lt ON lt.FIN_IdAgricultor = a.AGR_Idagricultor AND lt.rn = 1
        LEFT JOIN Tecnicos t ON lt.CUL_IdTecnico = TRY_CAST(t.TEC_IdTecnico AS INT)
        WHERE r.REC_Provincia != '' AND r.REC_FechaBaja = '1900-01-01'
          AND UPPER(LTRIM(RTRIM(f.FIN_Provincia))) IN ({prov_in})
    """)

    # Deduplicate and build stats
    seen: set = set()
    recintos = []
    tec_stats: dict = {}
    for r in rows:
        key = f"{r['prov']}-{r['mun']}-{r['pol']}-{r['par']}-{r['rec']}"
        if "None" in key or key in seen:
            continue
        seen.add(key)
        recintos.append({
            "key": key, "municipio": r["munNombre"] or "",
            "agrId": r["agrId"], "agrNombre": r["agrNombre"],
            "tecId": r["tecId"], "tecNombre": r["tecNombre"] or "Sin técnico",
            "tipo": "eco" if r["agrId"] >= 10000 else "conv",
        })
        tk = r["tecId"] or 0
        if tk not in tec_stats:
            tec_stats[tk] = {"id": r["tecId"], "nombre": r["tecNombre"] or "Sin técnico",
                             "eco_agr": set(), "eco_rec": 0, "conv_agr": set(), "conv_rec": 0, "municipios": set()}
        tipo = "eco" if r["agrId"] >= 10000 else "conv"
        tec_stats[tk][f"{tipo}_agr"].add(r["agrId"])
        tec_stats[tk][f"{tipo}_rec"] += 1
        if r["munNombre"]:
            tec_stats[tk]["municipios"].add(r["munNombre"])

    tecnicos = sorted([
        {"id": t["id"], "nombre": t["nombre"],
         "eco": {"agricultores": len(t["eco_agr"]), "recintos": t["eco_rec"]},
         "conv": {"agricultores": len(t["conv_agr"]), "recintos": t["conv_rec"]},
         "totalAgricultores": len(t["eco_agr"]) + len(t["conv_agr"]),
         "totalRecintos": t["eco_rec"] + t["conv_rec"],
         "municipios": sorted(t["municipios"])}
        for t in tec_stats.values()
    ], key=lambda x: x["totalRecintos"], reverse=True)

    prov_label = ", ".join(code_to_name.get(int(c), str(c)) for c in provincias)
    return {
        "recintos": recintos,
        "tecnicos": tecnicos,
        "meta": {"provincias": prov_label, "totalRecintos": len(recintos)},
    }


# ---------------------------------------------------------------------------
# FONDOS OPERATIVOS (NetOpfh_26)
# ---------------------------------------------------------------------------


@mcp.tool()
def opfh_recintos() -> dict:
    """Lista recintos OPFH con sus subrecintos y agricultores asignados (solo activos)."""
    rows = _query("""
        SELECT r.REC_CodRecinto, r.REC_IdProvincia, r.REC_IdMunicipio,
               r.REC_IdPoligono, r.REC_IdParcela, r.REC_IdRecinto, r.REC_Metros,
               s.SUR_Id, s.SUR_CodSubRecinto, s.SUR_SubRecinto, s.SUR_Metros,
               a.AGR_Nombre AS agrNombre
        FROM NetOpfh_26.dbo.Recintos r
        INNER JOIN NetOpfh_26.dbo.SubRecintos s ON r.REC_CodRecinto = s.SUR_CodRecinto
        LEFT JOIN TecnicosNet.dbo.Agricultores a ON s.SUR_IdAgricultor = a.AGR_Idagricultor
        WHERE r.REC_CodRecinto IN (
            SELECT DISTINCT s2.SUR_CodRecinto
            FROM NetOpfh_26.dbo.SubRecintos s2
            INNER JOIN (SELECT DISTINCT CUL_IdAgriCultivo AS AGR_Idagricultor FROM TecnicosNet.dbo.Cultivos WHERE CUL_FechaFinalizaReal = '1900-01-01') aa ON s2.SUR_IdAgricultor = aa.AGR_Idagricultor
        )
        ORDER BY r.REC_CodRecinto, s.SUR_Id
    """)
    rec_map: dict = {}
    for r in rows:
        cod = r["REC_CodRecinto"]
        if cod not in rec_map:
            rec_map[cod] = {
                "cod": cod, "prov": r["REC_IdProvincia"], "mun": r["REC_IdMunicipio"],
                "pol": r["REC_IdPoligono"], "par": r["REC_IdParcela"], "rec": r["REC_IdRecinto"],
                "metros": r["REC_Metros"], "subrecintos": [],
            }
        if r["SUR_Id"] is not None:
            rec_map[cod]["subrecintos"].append({
                "id": r["SUR_Id"], "cod": r["SUR_CodSubRecinto"],
                "label": r["SUR_SubRecinto"], "metros": r["SUR_Metros"],
                "agrNombre": (r["agrNombre"] or "").strip(),
            })
    recintos = list(rec_map.values())
    return {"recintos": recintos, "meta": {"totalRecintos": len(recintos)}}


@mcp.tool()
def opfh_agricultores_por_nif(nifs: list[str]) -> dict:
    """Resuelve nombres de agricultores a partir de una lista de NIFs.

    Args:
        nifs: Lista de NIFs a buscar.
    """
    if not nifs:
        return {"agricultores": {}}
    safe_nifs = [n.strip().replace("'", "") for n in nifs[:2000]]
    in_clause = ", ".join(f"'{n}'" for n in safe_nifs)
    rows = _query(f"""
        SELECT LTRIM(RTRIM(AGR_Nif)) AS nif, LTRIM(RTRIM(AGR_Nombre)) AS nombre
        FROM TecnicosNet.dbo.Agricultores
        WHERE AGR_Nif IN ({in_clause}) AND AGR_Nombre != ''
    """)
    result: dict = {}
    for r in rows:
        if r["nif"] and r["nif"] not in result:
            result[r["nif"]] = r["nombre"]
    return {"agricultores": result}


@mcp.tool()
def control_presupuestario(incluir_obsoletas: bool = False) -> dict:
    """Control presupuestario MPO 2026: conceptos, socios, liberación, capas, objetivos.

    Args:
        incluir_obsoletas: Si True, incluye líneas marcadas como obsoletas.
    """
    obs_filter = "" if incluir_obsoletas else "AND frl.FRL_ObsoletaSN != 'S'"
    obs_where = "" if incluir_obsoletas else "WHERE frl.FRL_ObsoletaSN != 'S'"

    # Presupuestado por concepto
    pres_con = _query(f"""
        SELECT c.CPT_CodConcepto AS codigo, LTRIM(RTRIM(c.CPT_Nombre)) AS concepto,
            COUNT(*) AS lineas, SUM(frl.FRL_TotalSubvencionableLinea) AS presupuestado
        FROM Facturas_Lineas frl JOIN Conceptos c ON c.CPT_CodConcepto = frl.FRL_CodConcepto
        {obs_where} GROUP BY c.CPT_CodConcepto, c.CPT_Nombre ORDER BY SUM(frl.FRL_TotalSubvencionableLinea) DESC
    """, database="NetOpfh_26")

    # Ejecutado por concepto
    eje_con = _query("""
        SELECT c.CPT_CodConcepto AS codigo, LTRIM(RTRIM(c.CPT_Nombre)) AS concepto,
            COUNT(*) AS lineas, SUM(d.FDL_TotalSubvencionableLinea) AS ejecutado
        FROM FacturasDefinitivas_Lineas d
        JOIN FacturasDefinitivas fd ON fd.FDF_IdFactura = d.FDL_IdFactura
        JOIN Conceptos c ON c.CPT_CodConcepto = d.FDL_CodConcepto
        GROUP BY c.CPT_CodConcepto, c.CPT_Nombre ORDER BY SUM(d.FDL_TotalSubvencionableLinea) DESC
    """, database="NetOpfh_26")

    # Presupuestado por socio
    pres_soc = _query(f"""
        SELECT a.AGR_Idagricultor AS id, LTRIM(RTRIM(a.AGR_Nombre)) AS nombre,
            COUNT(*) AS lineas, SUM(frl.FRL_TotalSubvencionableLinea) AS presupuestado
        FROM Facturas_Lineas frl
        JOIN Facturas f ON f.FRA_IdFactura = frl.FRL_IdFactura
        JOIN AA_Replica_Agricultores a ON a.AGR_Idagricultor = f.FRA_IdAgricultor
        {obs_where} GROUP BY a.AGR_Idagricultor, a.AGR_Nombre ORDER BY SUM(frl.FRL_TotalSubvencionableLinea) DESC
    """, database="NetOpfh_26")

    # Ejecutado por socio
    eje_soc = _query("""
        SELECT a.AGR_Idagricultor AS id, LTRIM(RTRIM(a.AGR_Nombre)) AS nombre,
            COUNT(*) AS lineas, SUM(d.FDL_TotalSubvencionableLinea) AS ejecutado
        FROM FacturasDefinitivas_Lineas d
        JOIN FacturasDefinitivas fd ON fd.FDF_IdFactura = d.FDL_IdFactura
        JOIN Facturas_Lineas frl ON frl.FRL_Id = d.FDL_IdLineaProforma
        JOIN Facturas f ON f.FRA_IdFactura = frl.FRL_IdFactura
        JOIN AA_Replica_Agricultores a ON a.AGR_Idagricultor = f.FRA_IdAgricultor
        GROUP BY a.AGR_Idagricultor, a.AGR_Nombre ORDER BY SUM(d.FDL_TotalSubvencionableLinea) DESC
    """, database="NetOpfh_26")

    # Liberación línea a línea
    liberacion = _query(f"""
        SELECT frl.FRL_Id AS id, a.AGR_Idagricultor AS codAgr, LTRIM(RTRIM(a.AGR_Nombre)) AS socio,
            c.CPT_CodConcepto AS concepto, LTRIM(RTRIM(c.CPT_Nombre)) AS descripcion,
            frl.FRL_TotalSubvencionableLinea AS proforma,
            dg.defTotal AS definitiva,
            f.FRA_IdFactura AS idFactura, f.FRA_IdMAC AS idMac
        FROM Facturas_Lineas frl
        JOIN Facturas f ON f.FRA_IdFactura = frl.FRL_IdFactura
        JOIN AA_Replica_Agricultores a ON a.AGR_Idagricultor = f.FRA_IdAgricultor
        JOIN Conceptos c ON c.CPT_CodConcepto = frl.FRL_CodConcepto
        LEFT JOIN (
            SELECT d.FDL_IdLineaProforma, SUM(d.FDL_TotalSubvencionableLinea) AS defTotal
            FROM FacturasDefinitivas_Lineas d JOIN FacturasDefinitivas fd ON fd.FDF_IdFactura = d.FDL_IdFactura
            GROUP BY d.FDL_IdLineaProforma
        ) dg ON dg.FDL_IdLineaProforma = frl.FRL_Id
        WHERE (frl.FRL_TotalSubvencionableLinea > 0 OR dg.defTotal > 0) {obs_filter}
        ORDER BY frl.FRL_TotalSubvencionableLinea DESC
    """, database="NetOpfh_26")

    lib_rows = []
    for r in liberacion:
        pro = r["proforma"] or 0
        defi = r["definitiva"] or 0
        estado = "PENDIENTE"
        if defi > 0 and defi == pro:
            estado = "EJECUTADO IGUAL"
        elif defi > 0 and defi < pro:
            estado = "LIBERADO"
        elif defi > pro > 0:
            estado = "EJECUTADO CON AUMENTO"
        lib_rows.append({**r, "liberado": pro - defi, "estado": estado})

    # Capas
    pres_capa = _query(f"""
        SELECT CASE WHEN frl.FRL_CodConcepto LIKE '2.f%' THEN 'retiradas'
                    WHEN frl.FRL_CodConcepto LIKE '2.%' THEN 'crisis' ELSE 'gastos50' END AS capa,
            SUM(frl.FRL_TotalSubvencionableLinea) AS presupuestado
        FROM Facturas_Lineas frl {obs_where}
        GROUP BY CASE WHEN frl.FRL_CodConcepto LIKE '2.f%' THEN 'retiradas' WHEN frl.FRL_CodConcepto LIKE '2.%' THEN 'crisis' ELSE 'gastos50' END
    """, database="NetOpfh_26")
    eje_capa = _query("""
        SELECT CASE WHEN d.FDL_CodConcepto LIKE '2.f%' THEN 'retiradas'
                    WHEN d.FDL_CodConcepto LIKE '2.%' THEN 'crisis' ELSE 'gastos50' END AS capa,
            SUM(d.FDL_TotalSubvencionableLinea) AS ejecutado
        FROM FacturasDefinitivas_Lineas d JOIN FacturasDefinitivas fd ON fd.FDF_IdFactura = d.FDL_IdFactura
        GROUP BY CASE WHEN d.FDL_CodConcepto LIKE '2.f%' THEN 'retiradas' WHEN d.FDL_CodConcepto LIKE '2.%' THEN 'crisis' ELSE 'gastos50' END
    """, database="NetOpfh_26")
    capas: dict = {}
    for r in pres_capa:
        capas[r["capa"]] = {"presupuestado": r["presupuestado"] or 0, "ejecutado": 0}
    for r in eje_capa:
        if r["capa"] not in capas:
            capas[r["capa"]] = {"presupuestado": 0, "ejecutado": 0}
        capas[r["capa"]]["ejecutado"] = r["ejecutado"] or 0

    # Anualidad
    anualidad = _query("SELECT LimiteAyuda, LimiteGestionCrisis FROM Anualidad WHERE LTRIM(RTRIM(Anualidad)) = '2026'", database="NetOpfh_26")

    total_pres = sum(r.get("presupuestado", 0) or 0 for r in pres_con)
    total_eje = sum(r.get("ejecutado", 0) or 0 for r in eje_con)

    return {
        "conceptos": {"presupuestado": pres_con, "ejecutado": eje_con},
        "socios": {"presupuestado": pres_soc, "ejecutado": eje_soc},
        "liberacion": lib_rows,
        "totales": {"presupuestado": total_pres, "ejecutado": total_eje},
        "capas": capas,
        "anualidad": anualidad[0] if anualidad else None,
    }


if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "streamable-http"
    mcp.run(transport=transport)
