"""Tests para validación de respuestas de la API SIGPAC.

Garantiza que nunca se aceptan datos inválidos o incoherentes de SIGPAC.
"""

import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from mcp_server import (
    SigpacApiError,
    SigpacValidationError,
    _sigpac_recinfo,
    _validate_sigpac_response,
)


class TestValidateSigpacResponse:
    """Validación de estructura de respuesta SIGPAC."""

    def test_respuesta_valida(self, sigpac_response_valid):
        result = _validate_sigpac_response(sigpac_response_valid, 4, 79, 22, 75, 1)
        assert result["superficie"] == 0.4442
        assert result["uso_sigpac"] == "IV"

    def test_campos_faltantes(self, sigpac_response_missing_superficie):
        with pytest.raises(SigpacValidationError, match="faltan campos.*superficie"):
            _validate_sigpac_response(sigpac_response_missing_superficie, 4, 79, 22, 75, 1)

    def test_superficie_none(self):
        data = {
            "provincia": 4, "municipio": 79, "poligono": 22,
            "parcela": 75, "recinto": 1, "superficie": None,
        }
        with pytest.raises(SigpacValidationError, match="superficie.*None"):
            _validate_sigpac_response(data, 4, 79, 22, 75, 1)

    def test_superficie_negativa(self, sigpac_response_negative_superficie):
        with pytest.raises(SigpacValidationError, match="negativa"):
            _validate_sigpac_response(sigpac_response_negative_superficie, 4, 79, 22, 75, 1)

    def test_superficie_no_numerica(self):
        data = {
            "provincia": 4, "municipio": 79, "poligono": 22,
            "parcela": 75, "recinto": 1, "superficie": "abc",
        }
        with pytest.raises(SigpacValidationError, match="no es numérico"):
            _validate_sigpac_response(data, 4, 79, 22, 75, 1)

    def test_recinto_equivocado(self, sigpac_response_wrong_recinto):
        """SIGPAC devuelve datos de un recinto diferente al pedido."""
        with pytest.raises(SigpacValidationError, match="devolvió recinto 99.*pidió 1"):
            _validate_sigpac_response(sigpac_response_wrong_recinto, 4, 79, 22, 75, 1)

    def test_provincia_equivocada(self):
        data = {
            "provincia": 11, "municipio": 79, "poligono": 22,
            "parcela": 75, "recinto": 1, "superficie": 0.5,
        }
        with pytest.raises(SigpacValidationError, match="provincia 11.*pidió 4"):
            _validate_sigpac_response(data, 4, 79, 22, 75, 1)

    def test_municipio_equivocado(self):
        data = {
            "provincia": 4, "municipio": 99, "poligono": 22,
            "parcela": 75, "recinto": 1, "superficie": 0.5,
        }
        with pytest.raises(SigpacValidationError, match="municipio 99.*pidió 79"):
            _validate_sigpac_response(data, 4, 79, 22, 75, 1)

    def test_respuesta_vacia(self):
        with pytest.raises(SigpacValidationError, match="faltan campos"):
            _validate_sigpac_response({}, 4, 79, 22, 75, 1)

    def test_superficie_cero_es_valida(self):
        """Superficie 0 es válida (puede ser un recinto sin uso)."""
        data = {
            "provincia": 4, "municipio": 79, "poligono": 22,
            "parcela": 75, "recinto": 1, "superficie": 0.0,
        }
        result = _validate_sigpac_response(data, 4, 79, 22, 75, 1)
        assert result["superficie"] == 0.0


class TestSigpacRecinfo:
    """Consulta a la API SIGPAC con manejo de errores."""

    def _mock_urlopen(self, response_data):
        """Helper para mockear urllib.request.urlopen."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    @patch("mcp_server.urllib.request.urlopen")
    def test_respuesta_exitosa(self, mock_urlopen, sigpac_response_valid):
        mock_urlopen.return_value = self._mock_urlopen([sigpac_response_valid])
        result = _sigpac_recinfo(4, 79, 22, 75, 1)
        assert result["superficie"] == 0.4442

    @patch("mcp_server.urllib.request.urlopen")
    def test_lista_vacia_da_error(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_urlopen([])
        with pytest.raises(SigpacApiError, match="lista vacía"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_timeout_da_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("timeout")
        with pytest.raises(SigpacApiError, match="Error de red"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_http_404_da_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 404, "Not Found", {}, None
        )
        with pytest.raises(SigpacApiError, match="HTTP 404"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_http_500_da_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 500, "Internal Server Error", {}, None
        )
        with pytest.raises(SigpacApiError, match="HTTP 500"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_json_invalido_da_error(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"not json at all"
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        with pytest.raises(SigpacApiError, match="JSON válido"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_respuesta_valida_pero_recinto_equivocado(self, mock_urlopen, sigpac_response_wrong_recinto):
        mock_urlopen.return_value = self._mock_urlopen([sigpac_response_wrong_recinto])
        with pytest.raises(SigpacValidationError, match="devolvió recinto 99"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_respuesta_none_da_error(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_urlopen(None)
        with pytest.raises(SigpacApiError, match="inesperada"):
            _sigpac_recinfo(4, 79, 22, 75, 1)

    @patch("mcp_server.urllib.request.urlopen")
    def test_respuesta_dict_directo(self, mock_urlopen, sigpac_response_valid):
        """Algunos endpoints devuelven dict en vez de lista."""
        mock_urlopen.return_value = self._mock_urlopen(sigpac_response_valid)
        result = _sigpac_recinfo(4, 79, 22, 75, 1)
        assert result["superficie"] == 0.4442
