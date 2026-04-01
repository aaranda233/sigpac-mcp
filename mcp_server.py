"""MCP Server para SIGPAC: consulta de recintos, agricultores y comparativa de superficies.

Conecta a TecnicosNet y NetOpfh_26 en 192.168.2.36:1433.
Incluye consulta directa a la API SIGPAC para comparar superficies.

PRINCIPIO CRITICO: Nunca devolver datos falsos ni inventados.
Si algo falla, devolver error explícito. Nunca silenciar errores.
"""

import gzip
import json
import logging
import re
import ssl
import sys
import urllib.request

import pymssql
from mcp.server.fastmcp import FastMCP

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
        where.append("EXISTS (SELECT 1 FROM AA_Agricultores_Activos aa WHERE aa.AGR_Idagricultor = a.AGR_Idagricultor)")
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
        LEFT JOIN RecintosSigpac r ON r.REC_IdAgricultor = a.AGR_Idagricultor
            AND r.REC_FechaBaja = '1900-01-01'
        LEFT JOIN Fincas f ON r.REC_IdFinca = f.FIN_IdFinca
        LEFT JOIN Municipios m ON TRY_CAST(r.REC_Municipio AS INT) = m.MUN_Id
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
        LEFT JOIN Municipios m ON TRY_CAST(r.REC_Municipio AS INT) = m.MUN_Id
        WHERE r.REC_IdAgricultor = {int(agricultor_id)}
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
            LEFT JOIN Agricultores a ON r.REC_IdAgricultor = a.AGR_Idagricultor
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
        INNER JOIN AA_Agricultores_Activos aa ON s.SUR_IdAgricultor = aa.AGR_Idagricultor
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


if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "streamable-http"
    mcp.run(transport=transport)
