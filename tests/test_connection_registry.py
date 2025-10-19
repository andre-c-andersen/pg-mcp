"""Tests for ConnectionRegistry."""
import os
from unittest.mock import MagicMock, patch

import pytest

from pg_mcp.sql_driver import ConnectionRegistry


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
        yield mock


def test_discover_connections_single():
    """Test discovering a single DATABASE_URI connection."""
    registry = ConnectionRegistry()

    with patch.dict(os.environ, {"DATABASE_URI": "postgresql://localhost/test"}, clear=True):
        connections = registry.discover_connections()

    assert connections == {"default": "postgresql://localhost/test"}


def test_discover_connections_multiple():
    """Test discovering multiple DATABASE_URI_* connections."""
    registry = ConnectionRegistry()

    env = {
        "DATABASE_URI": "postgresql://localhost/default",
        "DATABASE_URI_APP": "postgresql://localhost/app",
        "DATABASE_URI_ETL": "postgresql://localhost/etl",
    }

    with patch.dict(os.environ, env, clear=True):
        connections = registry.discover_connections()

    assert connections == {
        "default": "postgresql://localhost/default",
        "app": "postgresql://localhost/app",
        "etl": "postgresql://localhost/etl",
    }


def test_discover_connections_none():
    """Test discovering connections when none are set."""
    registry = ConnectionRegistry()

    with patch.dict(os.environ, {}, clear=True):
        connections = registry.discover_connections()

    assert connections == {}


def test_discover_descriptions():
    """Test discovering DATABASE_DESC_* environment variables."""
    registry = ConnectionRegistry()

    env = {
        "DATABASE_DESC": "Main database",
        "DATABASE_DESC_APP": "Application database",
        "DATABASE_DESC_ETL": "ETL database",
    }

    with patch.dict(os.environ, env, clear=True):
        descriptions = registry.discover_descriptions()

    assert descriptions == {
        "default": "Main database",
        "app": "Application database",
        "etl": "ETL database",
    }


def test_discover_and_connect_success(mock_psycopg_connect):
    """Test successful connection discovery and validation."""
    registry = ConnectionRegistry()

    env = {
        "DATABASE_URI": "postgresql://localhost/test",
        "DATABASE_DESC": "Test database",
    }

    with patch.dict(os.environ, env, clear=True):
        registry.discover_and_connect()

    assert "default" in registry._connection_urls
    assert registry._connection_urls["default"] == "postgresql://localhost/test"
    assert registry._connection_valid["default"] is True
    assert registry._connection_errors["default"] is None
    assert registry._connection_descriptions["default"] == "Test database"

    # Verify psycopg.connect was called
    mock_psycopg_connect.assert_called_once_with("postgresql://localhost/test", autocommit=False)


def test_discover_and_connect_failure(mock_psycopg_connect):
    """Test connection discovery with connection failure."""
    registry = ConnectionRegistry()

    # Make the connection fail
    mock_psycopg_connect.side_effect = Exception("Connection refused")

    env = {"DATABASE_URI": "postgresql://localhost/test"}

    with patch.dict(os.environ, env, clear=True):
        registry.discover_and_connect()

    assert "default" in registry._connection_urls
    assert registry._connection_valid["default"] is False
    assert "Connection refused" in registry._connection_errors["default"]


def test_discover_and_connect_no_connections():
    """Test connection discovery when no connections are found."""
    registry = ConnectionRegistry()

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="No database connections found"):
            registry.discover_and_connect()


def test_get_connection_success(mock_psycopg_connect):
    """Test getting a valid connection URL."""
    registry = ConnectionRegistry()

    env = {"DATABASE_URI": "postgresql://localhost/test"}

    with patch.dict(os.environ, env, clear=True):
        registry.discover_and_connect()

    url = registry.get_connection("default")
    assert url == "postgresql://localhost/test"


def test_get_connection_not_found():
    """Test getting a connection that doesn't exist."""
    registry = ConnectionRegistry()
    registry._connection_urls = {"default": "postgresql://localhost/test"}

    with pytest.raises(ValueError, match="Connection 'nonexistent' not found"):
        registry.get_connection("nonexistent")


def test_get_connection_invalid(mock_psycopg_connect):
    """Test getting a connection that failed validation."""
    registry = ConnectionRegistry()

    # Make the connection fail
    mock_psycopg_connect.side_effect = Exception("Connection refused")

    env = {"DATABASE_URI": "postgresql://localhost/test"}

    with patch.dict(os.environ, env, clear=True):
        registry.discover_and_connect()

    with pytest.raises(ValueError, match="Connection 'default' is not available"):
        registry.get_connection("default")


def test_close_all():
    """Test closing all connections."""
    registry = ConnectionRegistry()
    registry._connection_urls = {"default": "postgresql://localhost/test"}
    registry._connection_valid = {"default": True}
    registry._connection_errors = {"default": None}
    registry._connection_descriptions = {"default": "Test"}

    registry.close_all()

    assert len(registry._connection_urls) == 0
    assert len(registry._connection_valid) == 0
    assert len(registry._connection_errors) == 0
    assert len(registry._connection_descriptions) == 0


def test_get_connection_names():
    """Test getting all connection names."""
    registry = ConnectionRegistry()
    registry._connection_urls = {
        "default": "postgresql://localhost/test1",
        "app": "postgresql://localhost/test2",
    }

    names = registry.get_connection_names()
    assert set(names) == {"default", "app"}


def test_get_connection_info():
    """Test getting connection information."""
    registry = ConnectionRegistry()
    registry._connection_urls = {
        "default": "postgresql://localhost/test1",
        "app": "postgresql://localhost/test2",
    }
    registry._connection_descriptions = {
        "default": "Main database",
    }

    info = registry.get_connection_info()

    # Sort by name for consistent comparison
    info = sorted(info, key=lambda x: x["name"])

    assert len(info) == 2
    assert info[0] == {"name": "app"}
    assert info[1] == {"name": "default", "description": "Main database"}
