"""Tests for ConnectionRegistry."""

import os
from unittest.mock import MagicMock
from unittest.mock import patch

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
        "DATABASE_URI_STAGE_EXAMPLE": "postgresql://localhost/stage_example",
        "DATABASE_URI_DEV_EXAMPLE": "postgresql://localhost/dev_example",
    }

    with patch.dict(os.environ, env, clear=True):
        connections = registry.discover_connections()

    assert connections == {
        "default": "postgresql://localhost/default",
        "stage_example": "postgresql://localhost/stage_example",
        "dev_example": "postgresql://localhost/dev_example",
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
        "DATABASE_DESC_STAGE_EXAMPLE": "Staging database example",
        "DATABASE_DESC_DEV_EXAMPLE": "Development database example",
    }

    with patch.dict(os.environ, env, clear=True):
        descriptions = registry.discover_descriptions()

    assert descriptions == {
        "default": "Main database",
        "stage_example": "Staging database example",
        "dev_example": "Development database example",
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

    assert "default" in registry._connection_urls  # type: ignore[reportPrivateUsage]
    assert registry._connection_urls["default"] == "postgresql://localhost/test"  # type: ignore[reportPrivateUsage]
    assert registry._connection_valid["default"] is True  # type: ignore[reportPrivateUsage]
    assert registry._connection_errors["default"] is None  # type: ignore[reportPrivateUsage]
    assert registry._connection_descriptions["default"] == "Test database"  # type: ignore[reportPrivateUsage]

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

    assert "default" in registry._connection_urls  # type: ignore[reportPrivateUsage]
    assert registry._connection_valid["default"] is False  # type: ignore[reportPrivateUsage]
    assert "Connection refused" in str(registry._connection_errors["default"])  # type: ignore[reportPrivateUsage]


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
    registry._connection_urls = {"default": "postgresql://localhost/test"}  # type: ignore[reportPrivateUsage]

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
    registry._connection_urls = {"default": "postgresql://localhost/test"}  # type: ignore[reportPrivateUsage]
    registry._connection_valid = {"default": True}  # type: ignore[reportPrivateUsage]
    registry._connection_errors = {"default": None}  # type: ignore[reportPrivateUsage]
    registry._connection_descriptions = {"default": "Test"}  # type: ignore[reportPrivateUsage]

    registry.close_all()

    assert len(registry._connection_urls) == 0  # type: ignore[reportPrivateUsage]
    assert len(registry._connection_valid) == 0  # type: ignore[reportPrivateUsage]
    assert len(registry._connection_errors) == 0  # type: ignore[reportPrivateUsage]
    assert len(registry._connection_descriptions) == 0  # type: ignore[reportPrivateUsage]


def test_get_connection_names():
    """Test getting all connection names."""
    registry = ConnectionRegistry()
    registry._connection_urls = {  # type: ignore[reportPrivateUsage]
        "default": "postgresql://localhost/test1",
        "app": "postgresql://localhost/test2",
    }

    names = registry.get_connection_names()
    assert set(names) == {"default", "app"}


def test_get_connection_info():
    """Test getting connection information."""
    registry = ConnectionRegistry()
    registry._connection_urls = {  # type: ignore[reportPrivateUsage]
        "default": "postgresql://localhost/test1",
        "app": "postgresql://localhost/test2",
    }
    registry._connection_descriptions = {  # type: ignore[reportPrivateUsage]
        "default": "Main database",
    }

    info = registry.get_connection_info()

    # Sort by name for consistent comparison
    info = sorted(info, key=lambda x: x["name"])

    assert len(info) == 2
    assert info[0] == {"name": "app"}
    assert info[1] == {"name": "default", "description": "Main database"}
