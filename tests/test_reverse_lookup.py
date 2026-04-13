"""Tests para búsqueda inversa SIGPAC (por coordenadas) e imagen→recinto."""

import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from mcp_server import (
    SigpacApiError,
    _sigpac_recinfo_by_point,
    buscar_recinto_por_coordenadas,
    extraer_coordenadas_imagen,
    imagen_a_recinto,
)


def _mock_urlopen_response(response_data):
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(response_data).encode("utf-8")
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


SAMPLE_RECINTO = {
    "provincia": 4,
    "municipio": 79,
    "poligono": 22,
    "parcela": 75,
    "recinto": 1,
    "superficie": 0.4442,
    "uso_sigpac": "IV",
    "coef_regadio": 1.0,
    "wkt": "POLYGON(...)",
}


class TestSigpacRecinfoByPoint:
    @patch("mcp_server.urllib.request.urlopen")
    def test_exito(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen_response([SAMPLE_RECINTO])
        result = _sigpac_recinfo_by_point(-2.12, 36.85)
        assert result["superficie"] == 0.4442
        assert result["provincia"] == 4

    @patch("mcp_server.urllib.request.urlopen")
    def test_lista_vacia(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen_response([])
        with pytest.raises(SigpacApiError, match="no encontró"):
            _sigpac_recinfo_by_point(-2.12, 36.85)

    @patch("mcp_server.urllib.request.urlopen")
    def test_error_red(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("timeout")
        with pytest.raises(SigpacApiError, match="Error de red"):
            _sigpac_recinfo_by_point(-2.12, 36.85)

    @patch("mcp_server.urllib.request.urlopen")
    def test_http_404(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 404, "Not Found", {}, None
        )
        with pytest.raises(SigpacApiError, match="HTTP 404"):
            _sigpac_recinfo_by_point(-2.12, 36.85)

    @patch("mcp_server.urllib.request.urlopen")
    def test_json_invalido(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"not json"
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        with pytest.raises(SigpacApiError, match="JSON válido"):
            _sigpac_recinfo_by_point(-2.12, 36.85)

    @patch("mcp_server.urllib.request.urlopen")
    def test_respuesta_incompleta(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen_response([{"provincia": 4}])
        with pytest.raises(SigpacApiError, match="incompleta"):
            _sigpac_recinfo_by_point(-2.12, 36.85)

    @patch("mcp_server.urllib.request.urlopen")
    def test_construye_url_correcta(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen_response([SAMPLE_RECINTO])
        _sigpac_recinfo_by_point(-2.12, 36.85)
        args, _ = mock_urlopen.call_args
        called_url = args[0].full_url
        assert "recinfobypoint/4258/-2.12/36.85.json" in called_url


class TestBuscarRecintoPorCoordenadas:
    @patch("mcp_server.urllib.request.urlopen")
    def test_exito(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen_response([SAMPLE_RECINTO])
        result = buscar_recinto_por_coordenadas(36.85, -2.12)
        assert result["superficie"] == 0.4442
        assert result["datos_fiables"] is True
        assert "wkt" not in result

    def test_coordenadas_fuera_espana(self):
        result = buscar_recinto_por_coordenadas(48.85, 2.35)
        assert "error" in result
        assert "fuera de España" in result["error"]

    @patch("mcp_server.urllib.request.urlopen")
    def test_api_falla(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("down")
        result = buscar_recinto_por_coordenadas(36.85, -2.12)
        assert "error" in result


class TestExtraerCoordenadasImagen:
    def test_archivo_no_existe(self):
        result = extraer_coordenadas_imagen("/tmp/no_existe_abc_123.jpg")
        assert "error" in result
        assert "no encontrada" in result["error"]


class TestImagenARecinto:
    def test_imagen_no_existe(self):
        result = imagen_a_recinto("/tmp/no_existe_xyz_999.jpg")
        assert "error" in result

    @patch("mcp_server.urllib.request.urlopen")
    @patch("mcp_server._extract_gps_from_image")
    def test_flujo_completo(self, mock_gps, mock_urlopen):
        mock_gps.return_value = {"latitud": 36.85, "longitud": -2.12, "altitud": 181.0}
        mock_urlopen.return_value = _mock_urlopen_response([SAMPLE_RECINTO])
        result = imagen_a_recinto("/tmp/foto.jpg")
        assert result["coordenadas"]["latitud"] == 36.85
        assert result["recinto"]["superficie"] == 0.4442
        assert result["datos_fiables"] is True
        assert "wkt" not in result["recinto"]

    @patch("mcp_server.urllib.request.urlopen")
    @patch("mcp_server._extract_gps_from_image")
    def test_gps_ok_sigpac_falla(self, mock_gps, mock_urlopen):
        mock_gps.return_value = {"latitud": 36.85, "longitud": -2.12}
        mock_urlopen.side_effect = urllib.error.URLError("down")
        result = imagen_a_recinto("/tmp/foto.jpg")
        assert "coordenadas" in result
        assert "error" in result
        assert "recinto" not in result

    @patch("mcp_server._extract_gps_from_image")
    def test_sin_gps_en_exif(self, mock_gps):
        mock_gps.side_effect = ValueError("no contiene datos GPS")
        result = imagen_a_recinto("/tmp/foto.jpg")
        assert "error" in result
        assert "GPS" in result["error"]
