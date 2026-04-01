"""Tests para los tools MCP del servidor SIGPAC.

Verifica que cada tool devuelve datos correctos o errores explícitos.
Nunca datos inventados, nunca silencia errores.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from mcp_server import (
    SigpacApiError,
    SigpacValidationError,
    comparar_recinto,
    consultar_sigpac,
    listar_diferencias_opfh,
    parse_cod_recinto,
    redistribuir_subrecintos,
    recintos_agricultor,
    subrecintos_recinto,
)


class TestParseCodRecinto:
    """Parsing de códigos OPFH."""

    def test_codigo_valido(self):
        assert parse_cod_recinto("4.66.24.1.3") == (4, 66, 24, 1, 3)

    def test_codigo_con_espacios(self):
        assert parse_cod_recinto("  4.66.24.1.3  ") == (4, 66, 24, 1, 3)

    def test_codigo_corto(self):
        with pytest.raises(ValueError, match="5 partes"):
            parse_cod_recinto("4.66.24.1")

    def test_codigo_largo(self):
        with pytest.raises(ValueError, match="5 partes"):
            parse_cod_recinto("4.66.24.1.3.99")

    def test_codigo_no_numerico(self):
        with pytest.raises(ValueError, match="numéricas"):
            parse_cod_recinto("4.abc.24.1.3")

    def test_codigo_vacio(self):
        with pytest.raises(ValueError, match="5 partes"):
            parse_cod_recinto("")

    def test_codigo_solo_puntos(self):
        with pytest.raises(ValueError):
            parse_cod_recinto("...")


class TestRedistribuirSubrecintos:
    """Redistribución proporcional de subrecintos."""

    def test_redistribucion_basica(self):
        subs = [
            {"sur_id": 1, "cod_sub": "A", "metros": 2000},
            {"sur_id": 2, "cod_sub": "B", "metros": 1500},
            {"sur_id": 3, "cod_sub": "C", "metros": 2500},
        ]
        result = redistribuir_subrecintos(subs, 7200, 6000)
        total = sum(p["propuesto"] for p in result)
        assert total == 7200, f"Suma {total} != 7200"

    def test_suma_exacta_siempre(self):
        """La suma de propuestos DEBE ser igual a sigpac_m2 exactamente."""
        subs = [
            {"sur_id": 1, "cod_sub": "A", "metros": 333},
            {"sur_id": 2, "cod_sub": "B", "metros": 333},
            {"sur_id": 3, "cod_sub": "C", "metros": 334},
        ]
        result = redistribuir_subrecintos(subs, 1001, 1000)
        total = sum(p["propuesto"] for p in result)
        assert total == 1001

    def test_total_bd_cero(self):
        subs = [{"sur_id": 1, "cod_sub": "A", "metros": 0}]
        result = redistribuir_subrecintos(subs, 1000, 0)
        assert result == []

    def test_subrecintos_vacios(self):
        result = redistribuir_subrecintos([], 1000, 500)
        assert result == []

    def test_diff_calculado_correctamente(self):
        subs = [{"sur_id": 1, "cod_sub": "A", "metros": 5000}]
        result = redistribuir_subrecintos(subs, 6000, 5000)
        assert result[0]["actual"] == 5000
        assert result[0]["propuesto"] == 6000
        assert result[0]["diff"] == 1000

    def test_proporciones_correctas(self):
        """50%/50% se mantiene en la redistribución."""
        subs = [
            {"sur_id": 1, "cod_sub": "A", "metros": 500},
            {"sur_id": 2, "cod_sub": "B", "metros": 500},
        ]
        result = redistribuir_subrecintos(subs, 2000, 1000)
        assert result[0]["propuesto"] == 1000
        assert result[1]["propuesto"] == 1000


class TestCompararRecinto:
    """Tests para el tool comparar_recinto."""

    def _mock_sigpac(self, data):
        """Helper que simula _sigpac_recinfo retornando data."""
        return patch("mcp_server._sigpac_recinfo", return_value=data)

    def _mock_query(self, rows):
        return patch("mcp_server._query", return_value=rows)

    def test_comparacion_exitosa(self, sigpac_response_valid, db_row_recinto):
        with self._mock_query([db_row_recinto]), self._mock_sigpac(sigpac_response_valid):
            result = comparar_recinto(4, 79, 22, 75, 1)

        assert result["datos_sigpac_fiables"] is True
        assert result["sigpac_m2"] == 4442
        assert result["bd_m2"] == 4400
        assert result["diferencia_m2"] == 42
        assert result["diferencia_pct"] == pytest.approx(0.95, abs=0.01)
        assert result["sigpac_uso"] == "IV"
        assert result["sigpac_coef_regadio"] == 1.0
        assert result["agricultor"] == "ESCANEZ GIMENEZ, JUAN ANTONIO"

    def test_error_sigpac_devuelve_error_explicito(self, db_row_recinto):
        with self._mock_query([db_row_recinto]):
            with patch("mcp_server._sigpac_recinfo", side_effect=SigpacApiError("timeout")):
                result = comparar_recinto(4, 79, 22, 75, 1)

        assert "error" in result
        assert result["datos_sigpac_fiables"] is False
        assert result["sigpac_m2"] is None
        assert result["bd_m2"] == 4400  # BD data still present

    def test_error_validacion_devuelve_error(self, db_row_recinto):
        with self._mock_query([db_row_recinto]):
            with patch("mcp_server._sigpac_recinfo", side_effect=SigpacValidationError("recinto equivocado")):
                result = comparar_recinto(4, 79, 22, 75, 1)

        assert "error" in result
        assert result["datos_sigpac_fiables"] is False

    def test_sin_datos_bd(self, sigpac_response_valid):
        with self._mock_query([]), self._mock_sigpac(sigpac_response_valid):
            result = comparar_recinto(4, 79, 22, 75, 1)

        assert result["bd_m2"] is None
        assert result["sigpac_m2"] == 4442
        assert result["diferencia_m2"] is None
        assert result["agricultor"] is None

    def test_error_bd_devuelve_error(self):
        from mcp_server import DatabaseError
        with patch("mcp_server._query", side_effect=DatabaseError("connection refused")):
            result = comparar_recinto(4, 79, 22, 75, 1)

        assert "error" in result


class TestConsultarSigpac:
    """Tests para el tool consultar_sigpac."""

    def test_consulta_exitosa(self, sigpac_response_valid):
        with patch("mcp_server._sigpac_recinfo", return_value=sigpac_response_valid):
            result = consultar_sigpac(4, 79, 22, 75, 1)

        assert result["datos_fiables"] is True
        assert result["superficie"] == 0.4442
        assert "wkt" not in result  # WKT debe filtrarse

    def test_error_api_devuelve_error(self):
        with patch("mcp_server._sigpac_recinfo", side_effect=SigpacApiError("HTTP 500")):
            result = consultar_sigpac(4, 79, 22, 75, 1)

        assert "error" in result
        assert result["datos_fiables"] is False

    def test_error_validacion_devuelve_error(self):
        with patch("mcp_server._sigpac_recinfo", side_effect=SigpacValidationError("campo faltante")):
            result = consultar_sigpac(4, 79, 22, 75, 1)

        assert "error" in result
        assert result["datos_fiables"] is False


class TestRecintosAgricultor:
    """Tests para el tool recintos_agricultor."""

    def test_devuelve_recintos(self):
        rows = [
            {
                "id": 1, "provincia": "4", "municipio": "79",
                "poligono": "22", "parcela": "75", "recinto": "1",
                "superficie_bd": 4400.0, "municipio_nombre": " NIJAR ",
                "finca_nombre": " FINCA 1 ",
            },
        ]
        with patch("mcp_server._query", return_value=rows):
            result = recintos_agricultor(21)

        assert len(result) == 1
        assert result[0]["municipio_nombre"] == "NIJAR"  # stripped

    def test_error_bd_devuelve_error(self):
        from mcp_server import DatabaseError
        with patch("mcp_server._query", side_effect=DatabaseError("connection refused")):
            result = recintos_agricultor(21)

        assert len(result) == 1
        assert "error" in result[0]


class TestSubrecintosRecinto:
    """Tests para el tool subrecintos_recinto."""

    def test_exitoso_con_redistribucion(self, subrecintos_rows, sigpac_response_valid):
        # sigpac_response_valid tiene superficie=0.4442 -> 4442 m²
        # rec_metros es 6000 (de subrecintos_rows)
        with patch("mcp_server._query", return_value=subrecintos_rows):
            with patch("mcp_server._sigpac_recinfo", return_value={
                **sigpac_response_valid,
                "provincia": 4, "municipio": 66, "poligono": 24, "parcela": 1, "recinto": 3,
                "superficie": 0.72,  # 7200 m²
            }):
                result = subrecintos_recinto("4.66.24.1.3")

        assert result["datos_sigpac_fiables"] is True
        assert result["sigpac_m2"] == 7200
        assert result["rec_metros_bd"] == 6000
        assert len(result["subrecintos"]) == 3
        assert "propuesta_redistribucion" in result
        total = sum(p["propuesto"] for p in result["propuesta_redistribucion"])
        assert total == 7200

    def test_codigo_invalido(self):
        result = subrecintos_recinto("4.66")
        assert "error" in result

    def test_error_sigpac_marca_no_fiable(self, subrecintos_rows):
        with patch("mcp_server._query", return_value=subrecintos_rows):
            with patch("mcp_server._sigpac_recinfo", side_effect=SigpacApiError("timeout")):
                result = subrecintos_recinto("4.66.24.1.3")

        assert result["datos_sigpac_fiables"] is False
        assert "error_sigpac" in result
        assert result["sigpac_m2"] is None
        assert "propuesta_redistribucion" not in result  # No proponer con datos no fiables

    def test_recinto_no_encontrado(self):
        with patch("mcp_server._query", return_value=[]):
            result = subrecintos_recinto("4.66.24.1.3")

        assert "error" in result


class TestListarDiferenciasOpfh:
    """Tests para el tool listar_diferencias_opfh."""

    def test_incluye_errores_sigpac(self):
        """Los recintos que fallan en SIGPAC deben reportarse, no omitirse."""
        db_rows = [
            {"cod": "4.66.24.1.3", "prov": 4, "mun": 66, "pol": 24, "par": 1, "rec": 3, "metros_bd": 5000},
        ]
        with patch("mcp_server._query", return_value=db_rows):
            with patch("mcp_server._sigpac_recinfo", side_effect=SigpacApiError("timeout")):
                result = listar_diferencias_opfh()

        # Debe haber al menos un aviso de error
        avisos = [r for r in result if "aviso" in r]
        assert len(avisos) == 1
        assert "1 recinto" in avisos[0]["aviso"]
