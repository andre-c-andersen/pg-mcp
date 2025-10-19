"""Tests for SqlDriver."""
from unittest.mock import MagicMock, patch

import pytest

from pg_mcp import SqlDriver


@pytest.fixture
def mock_psycopg_connect():
    """Mock psycopg.connect for testing."""
    with patch("pg_mcp.sql_driver.psycopg.connect") as mock:
        # Setup mock connection and cursor
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock.return_value = mock_conn
        yield mock, mock_conn, mock_cursor


def test_init_with_conn_url():
    """Test initializing SqlDriver with a connection URL via conn parameter."""
    driver = SqlDriver(conn="postgresql://localhost/test")
    assert driver.engine_url == "postgresql://localhost/test"


def test_init_with_engine_url():
    """Test initializing SqlDriver with engine_url parameter."""
    driver = SqlDriver(engine_url="postgresql://localhost/test")
    assert driver.engine_url == "postgresql://localhost/test"


def test_init_with_no_url():
    """Test that initializing without a URL raises ValueError."""
    with pytest.raises(ValueError, match="Either a connection URL"):
        SqlDriver()


def test_connect():
    """Test connect() returns the connection URL."""
    driver = SqlDriver(conn="postgresql://localhost/test")
    url = driver.connect()
    assert url == "postgresql://localhost/test"


def test_execute_query_with_results(mock_psycopg_connect):
    """Test executing a query that returns results."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    # Configure cursor to return results
    mock_cursor.description = ["id", "name"]
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"},
    ]
    mock_cursor.nextset.return_value = False

    driver = SqlDriver(conn="postgresql://localhost/test")
    results = driver.execute_query("SELECT * FROM test")

    assert results is not None
    assert len(results) == 2
    assert results[0].cells == {"id": 1, "name": "test1"}
    assert results[1].cells == {"id": 2, "name": "test2"}

    # Verify connection was created with correct URL
    mock.assert_called_once()
    assert "postgresql://localhost/test" in str(mock.call_args)


def test_execute_query_no_results(mock_psycopg_connect):
    """Test executing a query that returns no results (DDL/DML)."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    # Configure cursor to return no results
    mock_cursor.description = None
    mock_cursor.nextset.return_value = False

    driver = SqlDriver(conn="postgresql://localhost/test")
    results = driver.execute_query("DELETE FROM test")

    assert results is None


def test_execute_query_with_params(mock_psycopg_connect):
    """Test executing a query with parameters."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    mock_cursor.description = ["id"]
    mock_cursor.fetchall.return_value = [{"id": 1}]
    mock_cursor.nextset.return_value = False

    driver = SqlDriver(conn="postgresql://localhost/test")
    results = driver.execute_query("SELECT * FROM test WHERE id = %s", params=[1])

    assert results is not None
    assert len(results) == 1

    # Verify execute was called with params
    mock_cursor.execute.assert_called()


def test_execute_query_readonly(mock_psycopg_connect):
    """Test executing a query in read-only mode."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    mock_cursor.description = ["id"]
    mock_cursor.fetchall.return_value = [{"id": 1}]
    mock_cursor.nextset.return_value = False

    driver = SqlDriver(conn="postgresql://localhost/test")
    results = driver.execute_query("SELECT * FROM test", force_readonly=True)

    assert results is not None

    # Verify read-only transaction was started
    calls = [str(call) for call in mock_cursor.execute.call_args_list]
    assert any("BEGIN TRANSACTION READ ONLY" in call for call in calls)
    assert any("ROLLBACK" in call for call in calls)


def test_execute_query_writeable(mock_psycopg_connect):
    """Test executing a query in writeable mode."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    mock_cursor.description = None
    mock_cursor.nextset.return_value = False

    driver = SqlDriver(conn="postgresql://localhost/test")
    results = driver.execute_query("UPDATE test SET name = 'updated'", force_readonly=False)

    assert results is None

    # Verify commit was called
    calls = [str(call) for call in mock_cursor.execute.call_args_list]
    assert any("COMMIT" in call for call in calls)


def test_execute_query_error_handling(mock_psycopg_connect):
    """Test that query errors are properly raised."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    # Make execute raise an exception
    mock_cursor.execute.side_effect = Exception("Query error")

    driver = SqlDriver(conn="postgresql://localhost/test")

    with pytest.raises(Exception, match="Query error"):
        driver.execute_query("SELECT * FROM test")


def test_execute_query_error_rollback(mock_psycopg_connect):
    """Test that errors during read-only transactions trigger rollback."""
    mock, mock_conn, mock_cursor = mock_psycopg_connect

    # First call starts transaction, second call fails
    def execute_side_effect(*args, **kwargs):
        if "BEGIN" in str(args):
            return None
        raise Exception("Query error")

    mock_cursor.execute.side_effect = execute_side_effect

    driver = SqlDriver(conn="postgresql://localhost/test")

    with pytest.raises(Exception, match="Query error"):
        driver.execute_query("SELECT * FROM test", force_readonly=True)

    # Verify rollback was attempted
    mock_conn.rollback.assert_called()
