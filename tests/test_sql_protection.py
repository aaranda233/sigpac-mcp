"""Tests para protección contra SQL injection en run_select.

Verifica que NUNCA se ejecutan operaciones de escritura/modificación.
"""

import pytest

from mcp_server import run_select


class TestRunSelectProtection:
    """Verificar que run_select bloquea operaciones peligrosas."""

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO Agricultores VALUES (1, 'test')",
            "UPDATE Agricultores SET AGR_Nombre = 'hack'",
            "DELETE FROM Agricultores",
            "DROP TABLE Agricultores",
            "ALTER TABLE Agricultores ADD col INT",
            "CREATE TABLE hack (id INT)",
            "TRUNCATE TABLE Agricultores",
            "EXEC sp_help",
            "EXECUTE sp_help",
            "MERGE INTO Agricultores USING ...",
        ],
    )
    def test_operaciones_escritura_bloqueadas(self, sql):
        result = run_select(sql)
        assert len(result) == 1
        assert "error" in result[0]

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1; DROP TABLE Agricultores",
            "SELECT * FROM Agricultores; DELETE FROM Agricultores",
        ],
    )
    def test_multiples_sentencias_bloqueadas(self, sql):
        result = run_select(sql)
        assert "error" in result[0]

    def test_base_datos_no_permitida(self):
        result = run_select("SELECT 1", database="master")
        assert "error" in result[0]
        assert "no permitida" in result[0]["error"]

    def test_select_valido_sin_top_agrega_top(self):
        """Un SELECT sin TOP debe tener TOP 200 añadido."""
        # No podemos ejecutar realmente, pero verificamos que no lanza error de protección
        # Se lanzará DatabaseError por falta de conexión
        from mcp_server import DatabaseError
        from unittest.mock import patch

        with patch("mcp_server._query", return_value=[{"id": 1}]) as mock:
            result = run_select("SELECT * FROM Agricultores")
            # Verificar que se añadió TOP 200
            called_sql = mock.call_args[0][0]
            assert "TOP 200" in called_sql

    @pytest.mark.parametrize(
        "keyword",
        [
            "GRANT", "REVOKE", "DENY", "BACKUP", "RESTORE",
            "SHUTDOWN", "DBCC", "OPENROWSET", "OPENDATASOURCE",
            "XP_CMDSHELL", "SP_EXECUTESQL",
        ],
    )
    def test_keywords_peligrosos_bloqueados(self, keyword):
        result = run_select(f"SELECT * FROM Agricultores WHERE name = '{keyword}'")
        # Should be blocked because the keyword appears in the SQL
        assert "error" in result[0]

    def test_select_con_top_existente_no_duplica(self):
        from unittest.mock import patch
        with patch("mcp_server._query", return_value=[]) as mock:
            run_select("SELECT TOP 10 * FROM Agricultores")
            called_sql = mock.call_args[0][0]
            assert called_sql.count("TOP") == 1

    def test_with_cte_permitido(self):
        from unittest.mock import patch
        with patch("mcp_server._query", return_value=[]) as mock:
            run_select("WITH cte AS (SELECT 1 AS id) SELECT * FROM cte")
            assert mock.called
