"""SQL driver adapter for PostgreSQL connections."""

import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlunparse

import psycopg
from psycopg.rows import dict_row
from typing_extensions import LiteralString

from .env_utils import discover_database_connections
from .env_utils import discover_database_descriptions

logger = logging.getLogger(__name__)


def obfuscate_password(text: str | None) -> str | None:
    """
    Obfuscate password in any text containing connection information.
    Works on connection URLs, error messages, and other strings.
    """
    if text is None:
        return None

    if not text:
        return text

    # Try first as a proper URL
    try:
        parsed = urlparse(text)
        if parsed.scheme and parsed.netloc and parsed.password:
            # Replace password with asterisks in proper URL
            netloc = parsed.netloc.replace(parsed.password, "****")
            return urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        pass

    # Handle strings that contain connection strings but aren't proper URLs
    # Match postgres://user:password@host:port/dbname pattern
    url_pattern = re.compile(r"(postgres(?:ql)?:\/\/[^:]+:)([^@]+)(@[^\/\s]+)")
    text = re.sub(url_pattern, r"\1****\3", text)

    # Match connection string parameters (password=xxx)
    # This simpler pattern captures password without quotes
    param_pattern = re.compile(r'(password=)([^\s&;"\']+)', re.IGNORECASE)
    text = re.sub(param_pattern, r"\1****", text)

    # Match password in DSN format with single quotes
    dsn_single_quote = re.compile(r"(password\s*=\s*')([^']+)(')", re.IGNORECASE)
    text = re.sub(dsn_single_quote, r"\1****\3", text)

    # Match password in DSN format with double quotes
    dsn_double_quote = re.compile(r'(password\s*=\s*")([^"]+)(")', re.IGNORECASE)
    text = re.sub(dsn_double_quote, r"\1****\3", text)

    return text


class ConnectionRegistry:
    """Registry for managing multiple database connections."""

    def __init__(self):
        self._connection_urls: dict[str, str] = {}
        self._connection_descriptions: dict[str, str] = {}
        self._connection_valid: dict[str, bool] = {}
        self._connection_errors: dict[str, str | None] = {}

    def discover_connections(self) -> dict[str, str]:
        """
        Discover all DATABASE_URI_* environment variables.

        Returns:
            Dict mapping connection names to connection URLs
            - DATABASE_URI -> "default"
            - DATABASE_URI_STAGE_EXAMPLE -> "stage_example"
            - DATABASE_URI_DEV_EXAMPLE -> "dev_example"
        """
        return discover_database_connections()

    def discover_descriptions(self) -> dict[str, str]:
        """
        Discover all DATABASE_DESC_* environment variables.

        Returns:
            Dict mapping connection names to descriptions
            - DATABASE_DESC -> "default"
            - DATABASE_DESC_STAGE_EXAMPLE -> "stage_example"
            - DATABASE_DESC_DEV_EXAMPLE -> "dev_example"
        """
        return discover_database_descriptions()

    def discover_and_connect(self) -> None:
        """
        Discover all DATABASE_URI_* environment variables and test connections.
        """
        discovered = self.discover_connections()

        if not discovered:
            raise ValueError("No database connections found. Please set DATABASE_URI or DATABASE_URI_* environment variables.")

        logger.info(f"Discovered {len(discovered)} database connection(s): {', '.join(discovered.keys())}")

        # Store URLs and descriptions for reference
        self._connection_urls = discovered.copy()
        self._connection_descriptions = self.discover_descriptions()

        # Test each database URL
        for conn_name, url in discovered.items():
            try:
                # Test connection by opening, executing a simple query, and closing
                with psycopg.connect(url, autocommit=False) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")

                self._connection_valid[conn_name] = True
                self._connection_errors[conn_name] = None
                logger.info(f"Successfully tested connection to '{conn_name}'")
            except Exception as e:
                self._connection_valid[conn_name] = False
                self._connection_errors[conn_name] = str(e)
                error_msg = obfuscate_password(str(e))
                logger.warning(f"Failed to connect to '{conn_name}': {error_msg}")

    def get_connection(self, conn_name: str) -> str:
        """
        Get a connection URL by name.

        Args:
            conn_name: Connection name

        Returns:
            Connection URL string

        Raises:
            ValueError: If connection name doesn't exist
        """
        if conn_name not in self._connection_urls:
            available = ", ".join(f"'{name}'" for name in sorted(self._connection_urls.keys()))
            raise ValueError(f"Connection '{conn_name}' not found. Available connections: {available}")

        # Check if connection is valid
        if not self._connection_valid.get(conn_name, False):
            error_msg = self._connection_errors.get(conn_name) or "Unknown error"
            raise ValueError(f"Connection '{conn_name}' is not available: {obfuscate_password(error_msg)}")

        return self._connection_urls[conn_name]

    def close_all(self) -> None:
        """Clear all database connection state."""
        self._connection_urls.clear()
        self._connection_descriptions.clear()
        self._connection_valid.clear()
        self._connection_errors.clear()

    def get_connection_names(self) -> list[str]:
        """Get list of all connection names."""
        return list(self._connection_urls.keys())

    def get_connection_info(self) -> list[dict[str, str]]:
        """
        Get information about all configured connections.

        Returns:
            List of dicts with 'name' and optional 'description' for each connection
        """
        info = []
        for conn_name in sorted(self._connection_urls.keys()):
            conn_info = {"name": conn_name}
            if conn_name in self._connection_descriptions:
                conn_info["description"] = self._connection_descriptions[conn_name]
            info.append(conn_info)
        return info


class SqlDriver:
    """Adapter class that wraps a PostgreSQL connection with the interface expected by DTA."""

    @dataclass
    class RowResult:
        """Simple class to match the Griptape RowResult interface."""

        cells: dict[str, Any]

    def __init__(
        self,
        conn: Any = None,
        engine_url: str | None = None,
    ):
        """
        Initialize with a PostgreSQL connection URL.

        Args:
            conn: Connection URL string (for backward-compatible signature)
            engine_url: Connection URL string as an alternative
        """
        url = None
        if isinstance(conn, str):
            url = conn
        elif engine_url:
            url = engine_url

        if not url:
            raise ValueError("Either a connection URL (conn) or engine_url must be provided")

        self.engine_url = url

    def connect(self):
        """Return the connection URL."""
        if not self.engine_url:
            raise ValueError("Connection URL not set")
        return self.engine_url

    def execute_query(
        self,
        query: LiteralString,
        params: list[Any] | None = None,
        force_readonly: bool = False,
    ) -> list[RowResult] | None:
        """
        Execute a query and return results using a fresh connection per request.

        Args:
            query: SQL query to execute
            params: Query parameters
            force_readonly: Whether to enforce read-only mode

        Returns:
            List of RowResult objects or None on error
        """
        connection_url = self.engine_url
        if not connection_url:
            raise ValueError("Connection URL not available")

        # Open a fresh connection for this request
        # Note: dict_row is the correct factory but pyright doesn't recognize the type compatibility
        with psycopg.connect(connection_url, autocommit=False, row_factory=dict_row) as connection:  # type: ignore[arg-type]
            return self._execute_with_connection(connection, query, params, force_readonly=force_readonly)

    def _execute_with_connection(self, connection, query, params, force_readonly) -> list[RowResult] | None:
        """Execute query with the given connection."""
        transaction_started = False
        try:
            with connection.cursor() as cursor:
                # Start read-only transaction
                if force_readonly:
                    cursor.execute("BEGIN TRANSACTION READ ONLY")
                    transaction_started = True

                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # For multiple statements, move to the last statement's results
                while cursor.nextset():
                    pass

                if cursor.description is None:  # No results (like DDL statements)
                    if not force_readonly:
                        cursor.execute("COMMIT")
                    elif transaction_started:
                        cursor.execute("ROLLBACK")
                        transaction_started = False
                    return None

                # Get results from the last statement only
                rows = cursor.fetchall()

                # End the transaction appropriately
                if not force_readonly:
                    cursor.execute("COMMIT")
                elif transaction_started:
                    cursor.execute("ROLLBACK")
                    transaction_started = False

                return [SqlDriver.RowResult(cells=dict(row)) for row in rows]

        except Exception as e:
            # Try to roll back the transaction if it's still active
            if transaction_started:
                try:
                    connection.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error rolling back transaction: {rollback_error}")

            logger.error(f"Error executing query ({query}): {e}")
            raise e
