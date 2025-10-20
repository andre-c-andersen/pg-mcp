import asyncio
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from pg_mcp.safe_sql import SafeSqlDriver
from pg_mcp.server import AccessMode
from pg_mcp.server import get_sql_driver
from pg_mcp.sql_driver import SqlDriver


@pytest.fixture
def mock_db_connection():
    """Mock database connection URL."""
    return "postgresql://user:password@localhost/test"


@pytest.mark.parametrize(
    "access_mode,expected_driver_type",
    [
        (AccessMode.UNRESTRICTED, SqlDriver),
        (AccessMode.RESTRICTED, SafeSqlDriver),
    ],
)
def test_get_sql_driver_returns_correct_driver(access_mode, expected_driver_type, mock_db_connection):
    """Test that get_sql_driver returns the correct driver type based on access mode."""
    with (
        patch("pg_mcp.server.current_access_mode", access_mode),
        patch("pg_mcp.server.connection_registry.get_connection", return_value=mock_db_connection),
    ):
        driver = get_sql_driver(conn_name="default")
        assert isinstance(driver, expected_driver_type)

        # When in RESTRICTED mode, verify timeout is set
        if access_mode == AccessMode.RESTRICTED:
            assert isinstance(driver, SafeSqlDriver)
            assert driver.timeout == 30


def test_get_sql_driver_sets_timeout_in_restricted_mode(mock_db_connection):
    """Test that get_sql_driver sets the timeout in restricted mode."""
    with (
        patch("pg_mcp.server.current_access_mode", AccessMode.RESTRICTED),
        patch("pg_mcp.server.connection_registry.get_connection", return_value=mock_db_connection),
    ):
        driver = get_sql_driver(conn_name="default")
        assert isinstance(driver, SafeSqlDriver)
        assert driver.timeout == 30
        assert hasattr(driver, "sql_driver")


def test_get_sql_driver_in_unrestricted_mode_no_timeout(mock_db_connection):
    """Test that get_sql_driver in unrestricted mode is a regular SqlDriver."""
    with (
        patch("pg_mcp.server.current_access_mode", AccessMode.UNRESTRICTED),
        patch("pg_mcp.server.connection_registry.get_connection", return_value=mock_db_connection),
    ):
        driver = get_sql_driver(conn_name="default")
        assert isinstance(driver, SqlDriver)
        assert not hasattr(driver, "timeout")


@pytest.mark.asyncio
async def test_command_line_parsing():
    """Test that command-line arguments correctly set the access mode."""
    import sys

    from pg_mcp.server import main

    # Mock sys.argv and asyncio.run
    original_argv = sys.argv
    original_run = asyncio.run

    try:
        # Test with --access-mode=restricted
        sys.argv = [
            "pg_mcp",
            "postgresql://user:password@localhost/db",
            "--access-mode=restricted",
        ]
        asyncio.run = AsyncMock()

        with (
            patch("pg_mcp.server.current_access_mode", AccessMode.UNRESTRICTED),
            patch("pg_mcp.server.connection_registry.discover_and_connect", AsyncMock()),
            patch("pg_mcp.server.mcp.run_stdio_async", AsyncMock()),
            patch("pg_mcp.server.shutdown", AsyncMock()),
        ):
            # Reset the current_access_mode to UNRESTRICTED
            import pg_mcp.server

            pg_mcp.server.current_access_mode = AccessMode.UNRESTRICTED

            # Run main (partially mocked to avoid actual connection)
            try:
                main()
            except Exception:
                pass

            # Verify the mode was changed to RESTRICTED
            assert pg_mcp.server.current_access_mode == AccessMode.RESTRICTED

    finally:
        # Restore original values
        sys.argv = original_argv
        asyncio.run = original_run


@pytest.mark.asyncio
async def test_command_line_parsing_with_multiple_db_args():
    """Test that --db arguments correctly set multiple database connections."""
    import os
    import sys

    from pg_mcp.server import main

    # Mock sys.argv
    original_argv = sys.argv
    original_env = os.environ.copy()

    try:
        # Test with multiple --db arguments
        sys.argv = [
            "pg_mcp",
            "--db=prod=postgresql://user:password@prod-host/db",
            "--db=staging=postgresql://user:password@staging-host/db",
            "--access-mode=restricted",
        ]

        with (
            patch("pg_mcp.server.connection_registry.discover_and_connect", AsyncMock()),
            patch("pg_mcp.server.mcp.run", AsyncMock()),
        ):
            # Run main (partially mocked to avoid actual connection)
            try:
                main()
            except Exception:
                pass

            # Verify environment variables were set
            assert os.environ.get("DATABASE_URI_PROD") == "postgresql://user:password@prod-host/db"
            assert os.environ.get("DATABASE_URI_STAGING") == "postgresql://user:password@staging-host/db"

    finally:
        # Restore original values
        sys.argv = original_argv
        # Clean up environment variables
        for key in ["DATABASE_URI_PROD", "DATABASE_URI_STAGING"]:
            os.environ.pop(key, None)
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)
