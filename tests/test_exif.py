"""Tests para extracción de coordenadas GPS desde EXIF de imágenes."""

from unittest.mock import MagicMock, patch

import pytest

from mcp_server import (
    _coords_in_spain,
    _dms_to_decimal,
    _extract_gps_from_image,
)


class TestDmsToDecimal:
    def test_norte_positivo(self):
        assert _dms_to_decimal((36.0, 51.0, 5.80873), "N") == pytest.approx(36.851613, abs=1e-5)

    def test_sur_negativo(self):
        val = _dms_to_decimal((36.0, 51.0, 0.0), "S")
        assert val == pytest.approx(-36.85, abs=1e-5)

    def test_oeste_negativo(self):
        val = _dms_to_decimal((2.0, 7.0, 4.29645), "W")
        assert val == pytest.approx(-2.117860, abs=1e-5)

    def test_este_positivo(self):
        val = _dms_to_decimal((2.0, 0.0, 0.0), "E")
        assert val == pytest.approx(2.0, abs=1e-6)

    def test_dms_vacio_falla(self):
        with pytest.raises(ValueError, match="Formato DMS"):
            _dms_to_decimal((), "N")

    def test_dms_no_numerico_falla(self):
        with pytest.raises(ValueError, match="no numérico"):
            _dms_to_decimal(("a", "b", "c"), "N")


class TestCoordsInSpain:
    def test_almeria_dentro(self):
        assert _coords_in_spain(36.85, -2.12)

    def test_madrid_dentro(self):
        assert _coords_in_spain(40.4, -3.7)

    def test_canarias_dentro(self):
        assert _coords_in_spain(28.1, -15.4)

    def test_paris_fuera(self):
        assert not _coords_in_spain(48.85, 2.35)

    def test_marruecos_fuera(self):
        assert not _coords_in_spain(25.0, -5.0)


class TestExtractGpsFromImage:
    def _build_mock_exif(self, gps_dict):
        """Crea un mock de Image con el GPS IFD especificado."""
        exif = MagicMock()
        exif.__bool__ = lambda self: True
        exif.get_ifd = MagicMock(return_value=gps_dict)
        img = MagicMock()
        img.getexif = MagicMock(return_value=exif)
        return img

    def test_archivo_inexistente(self):
        with pytest.raises(FileNotFoundError, match="no encontrada"):
            _extract_gps_from_image("/tmp/no_existe_xyz_123.jpg")

    @patch("mcp_server.os.path.exists", return_value=True)
    @patch("mcp_server.PIL.Image.open")
    def test_gps_valido_almeria(self, mock_open, _exists):
        gps = {
            1: "N", 2: (36.0, 51.0, 5.80873),
            3: "W", 4: (2.0, 7.0, 4.29645),
            6: 181.65,
        }
        mock_open.return_value = self._build_mock_exif(gps)
        result = _extract_gps_from_image("/tmp/test.jpg")
        assert result["latitud"] == pytest.approx(36.851613, abs=1e-5)
        assert result["longitud"] == pytest.approx(-2.117860, abs=1e-5)
        assert result["altitud"] == 181.65

    @patch("mcp_server.os.path.exists", return_value=True)
    @patch("mcp_server.PIL.Image.open")
    def test_sin_exif(self, mock_open, _exists):
        exif = MagicMock()
        exif.__bool__ = lambda self: False
        img = MagicMock()
        img.getexif = MagicMock(return_value=exif)
        mock_open.return_value = img
        with pytest.raises(ValueError, match="no contiene metadatos EXIF"):
            _extract_gps_from_image("/tmp/test.jpg")

    @patch("mcp_server.os.path.exists", return_value=True)
    @patch("mcp_server.PIL.Image.open")
    def test_exif_sin_gps(self, mock_open, _exists):
        mock_open.return_value = self._build_mock_exif({})
        with pytest.raises(ValueError, match="no contiene datos GPS"):
            _extract_gps_from_image("/tmp/test.jpg")

    @patch("mcp_server.os.path.exists", return_value=True)
    @patch("mcp_server.PIL.Image.open")
    def test_gps_incompleto(self, mock_open, _exists):
        mock_open.return_value = self._build_mock_exif({1: "N", 2: (36.0, 51.0, 0.0)})
        with pytest.raises(ValueError, match="GPS incompletos"):
            _extract_gps_from_image("/tmp/test.jpg")

    @patch("mcp_server.os.path.exists", return_value=True)
    @patch("mcp_server.PIL.Image.open")
    def test_coordenadas_fuera_de_espana(self, mock_open, _exists):
        gps = {
            1: "N", 2: (48.0, 51.0, 0.0),
            3: "E", 4: (2.0, 21.0, 0.0),
        }
        mock_open.return_value = self._build_mock_exif(gps)
        with pytest.raises(ValueError, match="fuera de España"):
            _extract_gps_from_image("/tmp/paris.jpg")

    @patch("mcp_server.os.path.exists", return_value=True)
    @patch("mcp_server.PIL.Image.open")
    def test_sin_altitud(self, mock_open, _exists):
        gps = {
            1: "N", 2: (40.0, 24.0, 0.0),
            3: "W", 4: (3.0, 42.0, 0.0),
        }
        mock_open.return_value = self._build_mock_exif(gps)
        result = _extract_gps_from_image("/tmp/madrid.jpg")
        assert "altitud" not in result
        assert result["latitud"] == pytest.approx(40.4, abs=1e-3)
