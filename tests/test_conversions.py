"""Tests para conversiones de unidades y cálculos numéricos.

Garantiza que las conversiones hectáreas <-> m² son correctas y
que los cálculos de diferencia no producen valores falsos.
"""

import pytest

from mcp_server import calcular_diferencia, hectareas_a_m2


class TestHectareasAM2:
    """Conversión de hectáreas a metros cuadrados."""

    def test_conversion_basica(self):
        assert hectareas_a_m2(1.0) == 10000

    def test_conversion_cero(self):
        assert hectareas_a_m2(0.0) == 0

    def test_conversion_fraccion_pequena(self):
        """0.4442 ha = 4442 m² (caso real del screenshot)."""
        assert hectareas_a_m2(0.4442) == 4442

    def test_conversion_2908(self):
        """0.2908 ha = 2908 m² (caso real del screenshot)."""
        assert hectareas_a_m2(0.2908) == 2908

    def test_conversion_2423(self):
        """0.2423 ha = 2423 m² (caso real del screenshot)."""
        assert hectareas_a_m2(0.2423) == 2423

    def test_redondeo_correcto(self):
        """0.44425 ha -> 4442 m² o 4443 m², nunca otro valor."""
        result = hectareas_a_m2(0.44425)
        assert result == 4442 or result == 4443

    def test_precision_punto_flotante(self):
        """Verifica que no hay errores de punto flotante."""
        assert hectareas_a_m2(0.0001) == 1
        # 0.00005 ha = 0.5 m² -> Python banker's rounding = 0
        assert hectareas_a_m2(0.00005) == 0
        assert hectareas_a_m2(0.00004) == 0
        # 0.00015 ha = 1.5 m² -> floating point makes it ~1.4999... -> rounds to 1
        assert hectareas_a_m2(0.00015) in (1, 2)  # either is acceptable at this precision

    def test_valores_grandes(self):
        """Fincas grandes pero realistas."""
        assert hectareas_a_m2(50.0) == 500000
        assert hectareas_a_m2(100.0) == 1000000

    def test_devuelve_int(self):
        """Siempre devuelve entero, nunca float."""
        result = hectareas_a_m2(0.4442)
        assert isinstance(result, int)

    @pytest.mark.parametrize(
        "hectareas,esperado",
        [
            (0.4442, 4442),
            (0.2908, 2908),
            (0.2423, 2423),
            (0.44, 4400),
            (0.29, 2900),
            (0.28, 2800),
            (1.5, 15000),
            (0.001, 10),
        ],
    )
    def test_valores_conocidos(self, hectareas, esperado):
        assert hectareas_a_m2(hectareas) == esperado


class TestCalcularDiferencia:
    """Cálculo de diferencia entre SIGPAC y BD."""

    def test_sin_diferencia(self):
        result = calcular_diferencia(4442, 4442)
        assert result["diferencia_m2"] == 0
        assert result["diferencia_pct"] == 0.0

    def test_sigpac_mayor(self):
        """SIGPAC tiene más m² que BD."""
        result = calcular_diferencia(4442, 4400)
        assert result["diferencia_m2"] == 42
        assert result["diferencia_pct"] == pytest.approx(0.95, abs=0.01)

    def test_sigpac_menor(self):
        """SIGPAC tiene menos m² que BD (caso 78.2 del screenshot)."""
        result = calcular_diferencia(2423, 2800)
        assert result["diferencia_m2"] == -377
        assert result["diferencia_pct"] < 0

    def test_bd_cero(self):
        """BD tiene 0 m²: porcentaje debe ser None (evitar división por cero)."""
        result = calcular_diferencia(4442, 0)
        assert result["diferencia_m2"] == 4442
        assert result["diferencia_pct"] is None

    def test_ambos_cero(self):
        result = calcular_diferencia(0, 0)
        assert result["diferencia_m2"] == 0
        assert result["diferencia_pct"] is None

    def test_diferencia_grande(self):
        """Diferencia > 100% debe calcularse correctamente."""
        result = calcular_diferencia(10000, 3000)
        assert result["diferencia_m2"] == 7000
        assert result["diferencia_pct"] == pytest.approx(233.33, abs=0.01)
