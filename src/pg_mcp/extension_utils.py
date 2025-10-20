"""Utilities for working with PostgreSQL extensions."""

import logging
from dataclasses import dataclass
from typing import Literal

from .safe_sql import SafeSqlDriver
from .sql_driver import SqlDriver

logger = logging.getLogger(__name__)

# Single global PostgreSQL version cache
# TODO: If we support multiple connections in the future, this should be connection-specific
_POSTGRES_VERSION = None


@dataclass
class ExtensionStatus:
    """Status of an extension."""

    is_installed: bool
    is_available: bool
    name: str
    message: str
    default_version: str | None


def reset_postgres_version_cache() -> None:
    """Reset the PostgreSQL version cache. Primarily used for testing."""
    global _POSTGRES_VERSION
    _POSTGRES_VERSION = None


def get_postgres_version(sql_driver: SqlDriver) -> int:
    """
    Get the major PostgreSQL version as an integer.

    Args:
        sql_driver: An instance of SqlDriver to execute queries

    Returns:
        The major PostgreSQL version as an integer (e.g., 16 for PostgreSQL 16.2)
        Returns 0 if the version cannot be determined
    """
    # Check if we have a cached version
    global _POSTGRES_VERSION
    if _POSTGRES_VERSION is not None:
        return _POSTGRES_VERSION

    try:
        rows = sql_driver.execute_query("SHOW server_version")
        if not rows:
            logger.warning("Could not determine PostgreSQL version")
            return 0

        version_string = rows[0].cells["server_version"]
        # Extract the major version (before the first dot)
        major_version = version_string.split(".")[0]
        version = int(major_version)

        # Cache the version globally
        _POSTGRES_VERSION = version

        return version
    except Exception as e:
        raise ValueError("Error determining PostgreSQL version") from e


def check_postgres_version_requirement(sql_driver: SqlDriver, min_version: int, feature_name: str) -> tuple[bool, str]:
    """
    Check if the PostgreSQL version meets the minimum requirement.

    Args:
        sql_driver: An instance of SqlDriver to execute queries
        min_version: The minimum required PostgreSQL version
        feature_name: Name of the feature that requires this version

    Returns:
        A tuple of (meets_requirement, message)
    """
    pg_version = get_postgres_version(sql_driver)

    if pg_version >= min_version:
        return True, f"PostgreSQL version {pg_version} meets the requirement for {feature_name}"

    return False, (
        f"This feature ({feature_name}) requires PostgreSQL {min_version} or later. Your current version is PostgreSQL {pg_version or 'unknown'}."
    )


def check_extension(
    sql_driver: SqlDriver,
    extension_name: str,
    include_messages: bool = True,
    message_type: Literal["plain", "markdown"] = "plain",
) -> ExtensionStatus:
    """
    Check if a PostgreSQL extension is installed or available.

    Args:
        sql_driver: An instance of SqlDriver to execute queries
        extension_name: Name of the extension to check
        include_messages: Whether to include user-friendly messages in the result
        message_type: Format for messages - 'plain' or 'markdown'

    Returns:
        ExtensionStatus with fields:
            - is_installed: True if the extension is installed
            - is_available: True if the extension is available but not installed
            - name: The extension name
            - message: A user-friendly message about the extension status
            - default_version: The default version of the extension if available
    """
    # Check if the extension is installed
    installed_result = SafeSqlDriver.execute_param_query(
        sql_driver,
        "SELECT extversion FROM pg_extension WHERE extname = {}",
        [extension_name],
    )

    # Initialize result
    result = ExtensionStatus(
        is_installed=False,
        is_available=False,
        name=extension_name,
        message="",
        default_version=None,
    )

    if installed_result and len(installed_result) > 0:
        # Extension is installed
        version = installed_result[0].cells.get("extversion", "unknown")
        result.is_installed = True
        result.is_available = True

        if include_messages:
            if message_type == "markdown":
                result.message = f"The **{extension_name}** extension (version {version}) is already installed."
            else:
                result.message = f"The {extension_name} extension (version {version}) is already installed."
    else:
        # Check if the extension is available but not installed
        available_result = SafeSqlDriver.execute_param_query(
            sql_driver,
            "SELECT default_version FROM pg_available_extensions WHERE name = {}",
            [extension_name],
        )

        if available_result and len(available_result) > 0:
            # Extension is available but not installed
            result.is_available = True
            result.default_version = available_result[0].cells.get("default_version")

            if include_messages:
                if message_type == "markdown":
                    result.message = (
                        f"The **{extension_name}** extension is available but not installed.\n\n"
                        f"You can install it by running: `CREATE EXTENSION {extension_name};`."
                    )
                else:
                    result.message = (
                        f"The {extension_name} extension is available but not installed.\n"
                        f"You can install it by running: CREATE EXTENSION {extension_name};"
                    )
        else:
            # Extension is not available
            if include_messages:
                if message_type == "markdown":
                    result.message = (
                        f"The **{extension_name}** extension is not available on this PostgreSQL server.\n\n"
                        f"To install it, you need to:\n"
                        f"1. Install the extension package on the server\n"
                        f"2. Run: `CREATE EXTENSION {extension_name};`"
                    )
                else:
                    result.message = (
                        f"The {extension_name} extension is not available on this PostgreSQL server.\n"
                        f"To install it, you need to:\n"
                        f"1. Install the extension package on the server\n"
                        f"2. Run: CREATE EXTENSION {extension_name};"
                    )

    return result
