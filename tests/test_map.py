"""Tests para generación de mapas de recintos SIGPAC."""

import io
from unittest.mock import patch

import pytest

from mcp_server import (
    SigpacApiError,
    SigpacValidationError,
    mapa_recinto,
    parse_wkt_polygon,
    render_recinto_map,
)
from mcp.types import TextContent


# ---------------------------------------------------------------------------
# parse_wkt_polygon
# ---------------------------------------------------------------------------


class TestParseWktPolygon:
    def test_simple_polygon(self):
        wkt = "POLYGON((-2.45 37.10, -2.44 37.10, -2.44 37.11, -2.45 37.11, -2.45 37.10))"
        coords = parse_wkt_polygon(wkt)
        assert len(coords) == 5
        assert coords[0] == (-2.45, 37.10)
        assert coords[-1] == (-2.45, 37.10)

    def test_polygon_with_whitespace(self):
        wkt = "  POLYGON (( -2.45 37.10 , -2.44 37.10 , -2.44 37.11 , -2.45 37.11 , -2.45 37.10 ))  "
        coords = parse_wkt_polygon(wkt)
        assert len(coords) == 5

    def test_none_raises(self):
        with pytest.raises(SigpacValidationError, match="vacío"):
            parse_wkt_polygon(None)

    def test_empty_string_raises(self):
        with pytest.raises(SigpacValidationError, match="vacío"):
            parse_wkt_polygon("")

    def test_whitespace_only_raises(self):
        with pytest.raises(SigpacValidationError, match="vacío"):
            parse_wkt_polygon("   ")

    def test_invalid_format_raises(self):
        with pytest.raises(SigpacValidationError, match="no contiene POLYGON"):
            parse_wkt_polygon("POINT(-2.45 37.10)")

    def test_too_few_vertices_raises(self):
        wkt = "POLYGON((-2.45 37.10, -2.44 37.10))"
        with pytest.raises(SigpacValidationError, match="menos de 3"):
            parse_wkt_polygon(wkt)

    def test_multipolygon_extracts_first(self):
        wkt = "MULTIPOLYGON(((-2.45 37.10, -2.44 37.10, -2.44 37.11, -2.45 37.11, -2.45 37.10)))"
        coords = parse_wkt_polygon(wkt)
        assert len(coords) == 5
        assert coords[0] == (-2.45, 37.10)

    def test_real_sigpac_wkt(self, sigpac_response_valid):
        """Verifica parsing con WKT del fixture (similar a respuesta real SIGPAC)."""
        coords = parse_wkt_polygon(sigpac_response_valid["wkt"])
        assert len(coords) == 5
        assert all(isinstance(lon, float) and isinstance(lat, float) for lon, lat in coords)


# ---------------------------------------------------------------------------
# render_recinto_map
# ---------------------------------------------------------------------------


class TestRenderRecintoMap:
    """Tests para render_recinto_map."""

    @pytest.fixture()
    def sample_coords(self):
        return [
            (-2.45, 37.10),
            (-2.44, 37.10),
            (-2.44, 37.11),
            (-2.45, 37.11),
            (-2.45, 37.10),
        ]

    def test_returns_valid_jpeg(self, sample_coords):
        """Verifica que devuelve bytes JPEG válidos."""
        data = render_recinto_map(sample_coords, 400, 300)
        assert isinstance(data, bytes)
        assert len(data) > 0
        # JPEG magic bytes (FFD8FF)
        assert data[:2] == b"\xff\xd8"

    def test_respects_dimensions(self, sample_coords):
        """Verifica que la imagen tiene las dimensiones solicitadas."""
        from PIL import Image as PILImage

        data = render_recinto_map(sample_coords, 320, 240)
        img = PILImage.open(io.BytesIO(data))
        assert img.size == (320, 240)

    def test_default_dimensions(self, sample_coords):
        """Verifica dimensiones por defecto 400x300."""
        from PIL import Image as PILImage

        data = render_recinto_map(sample_coords)
        img = PILImage.open(io.BytesIO(data))
        assert img.size == (400, 300)


# ---------------------------------------------------------------------------
# mapa_recinto tool
# ---------------------------------------------------------------------------


class TestMapaRecintoTool:
    """Tests para el tool mapa_recinto con API mockeada."""

    VALID_SIGPAC_RESPONSE = {
        "provincia": 4,
        "municipio": 79,
        "poligono": 22,
        "parcela": 75,
        "recinto": 1,
        "superficie": 0.4442,
        "uso_sigpac": "IV",
        "coef_regadio": 1.0,
        "pendiente_media": 19,
        "region": "AL",
        "wkt": "POLYGON((-2.4500 37.1000, -2.4490 37.1000, -2.4490 37.1010, -2.4500 37.1010, -2.4500 37.1000))",
    }

    @patch("mcp_server._sigpac_recinfo")
    @patch("mcp_server.render_recinto_map")
    def test_returns_text_with_url(self, mock_render, mock_api):
        mock_api.return_value = self.VALID_SIGPAC_RESPONSE
        mock_render.return_value = b"\xff\xd8\xff\xe0" + b"\x00" * 100

        result = mapa_recinto(4, 79, 22, 75, 1)

        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        assert "4/79/22/75/1" in result[0].text
        assert "0.4442" in result[0].text
        assert "IV" in result[0].text
        assert ".jpg" in result[0].text

    @patch("mcp_server._sigpac_recinfo")
    def test_api_error_returns_text_error(self, mock_api):
        mock_api.side_effect = SigpacApiError("HTTP 404")

        result = mapa_recinto(4, 79, 22, 75, 1)

        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        assert "Error" in result[0].text

    @patch("mcp_server._sigpac_recinfo")
    def test_missing_wkt_returns_error(self, mock_api):
        response = self.VALID_SIGPAC_RESPONSE.copy()
        response["wkt"] = None
        mock_api.return_value = response

        result = mapa_recinto(4, 79, 22, 75, 1)

        assert isinstance(result, list)
        assert len(result) == 1
        assert "Error" in result[0].text

    @patch("mcp_server._sigpac_recinfo")
    def test_invalid_wkt_returns_error(self, mock_api):
        response = self.VALID_SIGPAC_RESPONSE.copy()
        response["wkt"] = "POINT(-2.45 37.10)"
        mock_api.return_value = response

        result = mapa_recinto(4, 79, 22, 75, 1)

        assert isinstance(result, list)
        assert len(result) == 1
        assert "Error" in result[0].text

    @patch("mcp_server._sigpac_recinfo")
    @patch("mcp_server.render_recinto_map")
    def test_clamps_dimensions(self, mock_render, mock_api):
        mock_api.return_value = self.VALID_SIGPAC_RESPONSE
        mock_render.return_value = b"\xff\xd8\xff\xe0" + b"\x00" * 100

        mapa_recinto(4, 79, 22, 75, 1, ancho=50, alto=9999)

        mock_render.assert_called_once()
        call_args = mock_render.call_args
        assert call_args[0][1] == 200  # min clamp
        assert call_args[0][2] == 600  # max clamp
