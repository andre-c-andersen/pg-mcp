"""SQL utilities."""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("pg-mcp")
except importlib.metadata.PackageNotFoundError:
    # Package is not installed, try reading from pyproject.toml as fallback
    __version__ = "unknown"

from .bind_params import ColumnCollector
from .bind_params import SqlBindParams
from .bind_params import TableAliasVisitor
from .extension_utils import check_extension
from .extension_utils import check_postgres_version_requirement
from .extension_utils import get_postgres_version
from .extension_utils import reset_postgres_version_cache
from .safe_sql import SafeSqlDriver
from .sql_driver import ConnectionRegistry
from .sql_driver import SqlDriver
from .sql_driver import obfuscate_password

__all__ = [
    "__version__",
    "ColumnCollector",
    "ConnectionRegistry",
    "SafeSqlDriver",
    "SqlBindParams",
    "SqlDriver",
    "TableAliasVisitor",
    "check_extension",
    "check_postgres_version_requirement",
    "get_postgres_version",
    "obfuscate_password",
    "reset_postgres_version_cache",
]
