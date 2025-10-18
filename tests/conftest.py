import asyncio
import os
from collections.abc import Generator

import pytest
from dotenv import load_dotenv

from postgres_mcp.sql import reset_postgres_version_cache

load_dotenv()


# Define a custom event loop policy that handles cleanup better
@pytest.fixture(scope="session")
def event_loop_policy():
    """Create and return a custom event loop policy for tests."""
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture(scope="class")
def test_postgres_connection_string(request) -> Generator[tuple[str, str], None, None]:
    """
    Provides a PostgreSQL connection string for testing.

    Expects DATABASE_URI environment variable to be set.
    If not set, tests will be skipped.
    """
    db_uri = os.getenv("DATABASE_URI")
    if not db_uri:
        pytest.skip("DATABASE_URI environment variable not set. Tests require a PostgreSQL database.")

    # Return the connection string and a version identifier
    yield db_uri, "local"


@pytest.fixture(autouse=True)
def reset_pg_version_cache():
    """Reset the PostgreSQL version cache before each test."""
    reset_postgres_version_cache()
    yield
