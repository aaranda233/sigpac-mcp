"""MCP Server para SIGPAC: consulta de recintos, agricultores y comparativa de superficies.

Conecta a TecnicosNet y NetOpfh_26 en 192.168.2.36:1433.
Incluye consulta directa a la API SIGPAC para comparar superficies.
"""

import gzip
import json
import re
import ssl
import sys
import urllib.request

import pymssql
from mcp.server.fastmcp import FastMCP

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


def _connect(database: str = "TecnicosNet"):
    cfg = DB_CONFIG.copy()
    cfg["database"] = database
    return pymssql.connect(**cfg)


def _query(sql: str, database: str = "TecnicosNet") -> list[dict]:
    conn = _connect(database)
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def _sigpac_recinfo(prov, mun, pol, par, rec):
    """Consulta la API SIGPAC para obtener info oficial de un recinto."""
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
            return data[0] if isinstance(data, list) and data else data if data else None
    except Exception:
        return None


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
    rows = _query(sql)
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
    rows = _query(sql)
    for r in rows:
        for k, v in r.items():
            if isinstance(v, str):
                r[k] = v.strip()
    return rows


@mcp.tool()
def comparar_recinto(provincia: int, municipio: int, poligono: int, parcela: int, recinto: int) -> dict:
    """Compara la superficie de un recinto en la BD con la superficie oficial de SIGPAC.

    Args:
        provincia: Codigo de provincia (ej: 4 para Almeria).
        municipio: Codigo de municipio.
        poligono: Numero de poligono.
        parcela: Numero de parcela.
        recinto: Numero de recinto.
    """
    # BD
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
    bd = bd_rows[0] if bd_rows else None

    # SIGPAC API
    sigpac = _sigpac_recinfo(provincia, municipio, poligono, parcela, recinto)
    sigpac_m2 = round(sigpac["superficie"] * 10000) if sigpac and sigpac.get("superficie") is not None else None

    bd_m2 = round(float(bd["superficie_bd"])) if bd and bd["superficie_bd"] is not None else None

    result = {
        "recinto": f"{provincia}-{municipio}-{poligono}-{parcela}-{recinto}",
        "bd_m2": bd_m2,
        "sigpac_m2": sigpac_m2,
        "diferencia_m2": (sigpac_m2 - bd_m2) if sigpac_m2 is not None and bd_m2 is not None else None,
        "diferencia_pct": round((sigpac_m2 - bd_m2) / bd_m2 * 100, 2) if sigpac_m2 is not None and bd_m2 and bd_m2 > 0 else None,
        "agricultor": bd["agricultor"].strip() if bd and bd.get("agricultor") else None,
        "nif": bd["nif"].strip() if bd and bd.get("nif") else None,
    }
    if sigpac:
        result["sigpac_uso"] = sigpac.get("uso_sigpac")
        result["sigpac_coef_regadio"] = sigpac.get("coef_regadio")
        result["sigpac_pendiente"] = sigpac.get("pendiente_media")
        result["sigpac_region"] = sigpac.get("region")
    return result


@mcp.tool()
def listar_diferencias_opfh(min_pct: float = 0, max_results: int = 50) -> list[dict]:
    """Lista recintos de OPFH (agricultores activos) cuya superficie BD difiere de SIGPAC.

    Consulta NetOpfh_26 y compara con la API SIGPAC oficial. Solo devuelve los que tienen diferencia.
    ATENCION: puede tardar porque consulta la API SIGPAC para cada recinto.

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
    recintos = _query(sql)

    results = []
    for r in recintos:
        if len(results) >= max_results:
            break
        sigpac = _sigpac_recinfo(r["prov"], r["mun"], r["pol"], r["par"], r["rec"])
        if not sigpac or sigpac.get("superficie") is None:
            continue
        sigpac_m2 = round(sigpac["superficie"] * 10000)
        bd_m2 = r["metros_bd"] or 0
        if round(sigpac_m2) == round(bd_m2):
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

    return results


@mcp.tool()
def subrecintos_recinto(cod_recinto: str) -> dict:
    """Obtiene los subrecintos de un recinto en NetOpfh_26 con detalle de agricultor.

    Args:
        cod_recinto: Codigo del recinto en formato OPFH (ej: '4.66.24.1.3').
    """
    cod = cod_recinto.replace("'", "")
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
    rows = _query(sql)
    if not rows:
        return {"error": f"Recinto '{cod}' no encontrado"}

    rec_metros = rows[0]["rec_metros"]
    # Parse cod to get SIGPAC params
    parts = cod.split(".")
    sigpac = None
    if len(parts) >= 5:
        sigpac = _sigpac_recinfo(int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4]))

    sigpac_m2 = round(sigpac["superficie"] * 10000) if sigpac and sigpac.get("superficie") is not None else None

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
        "diferencia_m2": (sigpac_m2 - rec_metros) if sigpac_m2 is not None and rec_metros is not None else None,
        "subrecintos": subrecintos,
        "sum_subrecintos": sum(s["metros"] or 0 for s in subrecintos),
    }

    # Propuesta de redistribucion
    if sigpac_m2 is not None and rec_metros and round(sigpac_m2) != round(rec_metros):
        denom = rec_metros
        propuesta = []
        for s in subrecintos:
            new_m = round((s["metros"] / denom) * sigpac_m2) if denom > 0 else s["metros"]
            propuesta.append({
                "sur_id": s["sur_id"],
                "cod_sub": s["cod_sub"],
                "actual": s["metros"],
                "propuesto": new_m,
                "diff": new_m - s["metros"],
            })
        # Ajustar residuo al mayor
        sum_new = sum(p["propuesto"] for p in propuesta)
        remainder = sigpac_m2 - sum_new
        if remainder != 0 and propuesta:
            max_idx = max(range(len(propuesta)), key=lambda i: propuesta[i]["propuesto"])
            propuesta[max_idx]["propuesto"] += remainder
            propuesta[max_idx]["diff"] += remainder
        result["propuesta_redistribucion"] = propuesta

    return result


@mcp.tool()
def consultar_sigpac(provincia: int, municipio: int, poligono: int, parcela: int, recinto: int) -> dict:
    """Consulta la API oficial de SIGPAC para obtener datos de un recinto.

    Args:
        provincia: Codigo de provincia (ej: 4 para Almeria).
        municipio: Codigo de municipio.
        poligono: Numero de poligono.
        parcela: Numero de parcela.
        recinto: Numero de recinto.
    """
    info = _sigpac_recinfo(provincia, municipio, poligono, parcela, recinto)
    if not info:
        return {"error": "Recinto no encontrado en SIGPAC"}
    # Clean up response
    result = {}
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
    except Exception as e:
        return [{"error": str(e)}]


if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "streamable-http"
    mcp.run(transport=transport)
