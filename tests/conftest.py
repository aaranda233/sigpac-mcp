"""Fixtures compartidos para tests del MCP SIGPAC."""

import pytest


@pytest.fixture()
def sigpac_response_valid():
    """Respuesta SIGPAC real válida para recinto 4/79/22/75/1."""
    return {
        "provincia": 4,
        "municipio": 79,
        "agregado": 0,
        "zona": 0,
        "poligono": 22,
        "parcela": 75,
        "recinto": 1,
        "superficie": 0.4442,
        "uso_sigpac": "IV",
        "coef_regadio": 1.0,
        "pendiente_media": 19,
        "region": "AL",
        "wkt": "POLYGON((...))  ",
    }


@pytest.fixture()
def sigpac_response_missing_superficie():
    """Respuesta SIGPAC sin campo superficie."""
    return {
        "provincia": 4,
        "municipio": 79,
        "poligono": 22,
        "parcela": 75,
        "recinto": 1,
        "uso_sigpac": "IV",
        "coef_regadio": 1.0,
    }


@pytest.fixture()
def sigpac_response_wrong_recinto():
    """Respuesta SIGPAC que devuelve un recinto diferente al pedido."""
    return {
        "provincia": 4,
        "municipio": 79,
        "poligono": 22,
        "parcela": 75,
        "recinto": 99,
        "superficie": 0.4442,
        "uso_sigpac": "IV",
        "coef_regadio": 1.0,
    }


@pytest.fixture()
def sigpac_response_negative_superficie():
    """Respuesta SIGPAC con superficie negativa (inválida)."""
    return {
        "provincia": 4,
        "municipio": 79,
        "poligono": 22,
        "parcela": 75,
        "recinto": 1,
        "superficie": -0.5,
        "uso_sigpac": "IV",
        "coef_regadio": 1.0,
    }


@pytest.fixture()
def db_row_recinto():
    """Fila de BD típica para un recinto."""
    return {
        "REC_Id": 1001,
        "superficie_bd": 4400.0,
        "agricultor": "  ESCANEZ GIMENEZ, JUAN ANTONIO  ",
        "nif": " 12345678A ",
    }


@pytest.fixture()
def subrecintos_rows():
    """Filas de BD para subrecintos."""
    return [
        {
            "cod": "4.66.24.1.3",
            "rec_metros": 6000,
            "SUR_Id": 1,
            "cod_sub": "4.66.24.1.3.1",
            "sub": 1,
            "metros": 2000,
            "agr_id": 10,
            "agricultor": "  VARGAS  ",
        },
        {
            "cod": "4.66.24.1.3",
            "rec_metros": 6000,
            "SUR_Id": 2,
            "cod_sub": "4.66.24.1.3.2",
            "sub": 2,
            "metros": 1500,
            "agr_id": 20,
            "agricultor": "  BIO ALUMA  ",
        },
        {
            "cod": "4.66.24.1.3",
            "rec_metros": 6000,
            "SUR_Id": 3,
            "cod_sub": "4.66.24.1.3.3",
            "sub": 3,
            "metros": 2500,
            "agr_id": 30,
            "agricultor": "  GARCIA  ",
        },
    ]
