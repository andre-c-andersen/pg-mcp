"""SQL driver adapter for PostgreSQL connections."""

import logging
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlunparse

import psycopg
from psycopg.rows import dict_row
from typing_extensions import LiteralString

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


class DbConnPool:
    """Database connection manager that validates and stores connection URLs."""

    def __init__(self, connection_url: str | None = None):
        self.connection_url = connection_url
        self._is_valid = False
        self._last_error = None

    def test_connection(self, connection_url: str | None = None) -> None:
        """Test that the connection URL is valid by attempting to connect."""
        url = connection_url or self.connection_url
        self.connection_url = url

        if not url:
            self._is_valid = False
            self._last_error = "Database connection URL not provided"
            raise ValueError(self._last_error)

        try:
            # Test connection by opening, executing a simple query, and closing
            with psycopg.connect(url, autocommit=False) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")

            self._is_valid = True
            self._last_error = None
        except Exception as e:
            self._is_valid = False
            self._last_error = str(e)
            raise ValueError(f"Connection attempt failed: {obfuscate_password(str(e))}") from e

    def close(self) -> None:
        """No-op for compatibility. Connections are per-request, nothing to close."""
        pass

    @property
    def is_valid(self) -> bool:
        """Check if the connection URL is valid."""
        return self._is_valid

    @property
    def last_error(self) -> str | None:
        """Get the last error message."""
        return self._last_error


class ConnectionRegistry:
    """Registry for managing multiple database connections."""

    def __init__(self):
        self.connections: dict[str, DbConnPool] = {}
        self._connection_urls: dict[str, str] = {}
        self._connection_descriptions: dict[str, str] = {}

    def discover_connections(self) -> dict[str, str]:
        """
        Discover all DATABASE_URI_* environment variables.

        Returns:
            Dict mapping connection names to connection URLs
            - DATABASE_URI -> "default"
            - DATABASE_URI_APP -> "app"
            - DATABASE_URI_ETL -> "etl"
        """
        discovered = {}

        for env_var, url in os.environ.items():
            if env_var == "DATABASE_URI":
                discovered["default"] = url
            elif env_var.startswith("DATABASE_URI_"):
                # Extract postfix and lowercase it
                postfix = env_var[len("DATABASE_URI_") :]
                conn_name = postfix.lower()
                discovered[conn_name] = url

        return discovered

    def discover_descriptions(self) -> dict[str, str]:
        """
        Discover all DATABASE_DESC_* environment variables.

        Returns:
            Dict mapping connection names to descriptions
            - DATABASE_DESC -> "default"
            - DATABASE_DESC_APP -> "app"
            - DATABASE_DESC_ETL -> "etl"
        """
        descriptions = {}

        for env_var, desc in os.environ.items():
            if env_var == "DATABASE_DESC":
                descriptions["default"] = desc
            elif env_var.startswith("DATABASE_DESC_"):
                # Extract postfix and lowercase it
                postfix = env_var[len("DATABASE_DESC_") :]
                conn_name = postfix.lower()
                descriptions[conn_name] = desc

        return descriptions

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

        # Create connection managers and test each database URL
        for conn_name, url in discovered.items():
            conn_mgr = DbConnPool(url)
            self.connections[conn_name] = conn_mgr

            try:
                conn_mgr.test_connection()
                logger.info(f"Successfully tested connection to '{conn_name}'")
            except Exception as e:
                error_msg = obfuscate_password(str(e))
                logger.warning(f"Failed to connect to '{conn_name}': {error_msg}")
                logger.warning(f"Connection '{conn_name}' failed: {error_msg}")

    def get_connection(self, conn_name: str) -> DbConnPool:
        """
        Get a connection pool by name.

        Args:
            conn_name: Connection name (e.g., "default", "app", "etl")

        Returns:
            DbConnPool instance

        Raises:
            ValueError: If connection name doesn't exist
        """
        if conn_name not in self.connections:
            available = ", ".join(f"'{name}'" for name in sorted(self.connections.keys()))
            raise ValueError(f"Connection '{conn_name}' not found. Available connections: {available}")

        pool = self.connections[conn_name]

        # Check if connection is valid
        if not pool.is_valid:
            error_msg = pool.last_error or "Unknown error"
            raise ValueError(f"Connection '{conn_name}' is not available: {obfuscate_password(error_msg)}")

        return pool

    def close_all(self) -> None:
        """Close all database connections."""
        for conn_name, pool in self.connections.items():
            logger.info(f"Closing connection '{conn_name}'...")
            try:
                pool.close()
            except Exception as e:
                logger.error(f"Error closing connection '{conn_name}': {e}")

        self.connections.clear()
        self._connection_urls.clear()
        self._connection_descriptions.clear()

    def get_connection_names(self) -> list[str]:
        """Get list of all connection names."""
        return list(self.connections.keys())

    def get_connection_info(self) -> list[dict[str, str]]:
        """
        Get information about all configured connections.

        Returns:
            List of dicts with 'name' and optional 'description' for each connection
        """
        info = []
        for conn_name in sorted(self.connections.keys()):
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
        Initialize with a PostgreSQL connection or pool.

        Args:
            conn: PostgreSQL connection object or pool
            engine_url: Connection URL string as an alternative to providing a connection
        """
        if conn:
            self.conn = conn
            # Check if this is a connection pool
            self.is_pool = isinstance(conn, DbConnPool)
        elif engine_url:
            # Don't connect here since we need async connection
            self.engine_url = engine_url
            self.conn = None
            self.is_pool = False
        else:
            raise ValueError("Either conn or engine_url must be provided")

    def connect(self):
        if self.conn is not None:
            return self.conn
        if self.engine_url:
            self.conn = DbConnPool(self.engine_url)
            self.is_pool = True
            return self.conn
        else:
            raise ValueError("Connection not established. Either conn or engine_url must be provided")

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
        if self.conn is None:
            self.connect()
            if self.conn is None:
                raise ValueError("Connection not established")

        # Get the connection URL from the DbConnPool
        if not self.is_pool:
            raise ValueError("SqlDriver must be initialized with a DbConnPool instance")

        connection_url = self.conn.connection_url
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
