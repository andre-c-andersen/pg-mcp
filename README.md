<div align="center">

<img src="https://raw.githubusercontent.com/andre-c-andersen/pg-mcp/main/assets/postgres-mcp-lite.png" alt="Postgres MCP Lite Logo" width="600"/>

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![PyPI - Version](https://img.shields.io/pypi/v/pg-mcp)](https://pypi.org/project/pg-mcp/)
[![Twitter Follow](https://img.shields.io/twitter/follow/AndreCAndersen?style=flat)](https://x.com/AndreCAndersen)
[![Contributors](https://img.shields.io/github/contributors/andre-c-andersen/pg-mcp)](https://github.com/andre-c-andersen/pg-mcp/graphs/contributors)

<h3>A lightweight Postgres MCP server for schema exploration and SQL execution.</h3>

</div>

## Overview

**Postgres MCP Lite** is a lightweight, open-source Model Context Protocol (MCP) server for PostgreSQL. It provides AI assistants with essential database access: schema exploration and SQL execution.

This is a stripped-down fork of [postgres-mcp](https://github.com/crystaldba/postgres-mcp) by [Crystal DBA](https://www.linkedin.com/company/crystaldba/), focused on core functionality:

- **🗂️ Schema Exploration** - List schemas, tables, views, and get detailed object information including columns, constraints, and indexes.
- **⚡ SQL Execution** - Execute SQL queries with configurable access control.
- **🛡️ Safe SQL Execution** - Read-only mode with SQL parsing validation for production environments.
- **🔌 Multiple [Transports](https://modelcontextprotocol.io/docs/concepts/transports)** - Supports both stdio and SSE.

## Quick Start

### Prerequisites

Before getting started, ensure you have:
1. Access credentials for your database.
2. Python 3.12 or higher.

#### Access Credentials
 You can confirm your access credentials are valid by using `psql` or a GUI tool such as [pgAdmin](https://www.pgadmin.org/).


### Installation

If you have `pipx` installed you can install Postgres MCP Lite with:

```bash
pipx install pg-mcp
```

Otherwise, install Postgres MCP Lite with `uv`:

```bash
uv pip install pg-mcp
```

If you need to install `uv`, see the [uv installation instructions](https://docs.astral.sh/uv/getting-started/installation/).


### Configure Your AI Assistant

We provide full instructions for configuring Postgres MCP Lite with Claude Desktop.
Many MCP clients have similar configuration files, you can adapt these steps to work with the client of your choice.

#### Claude Desktop Configuration

You will need to edit the Claude Desktop configuration file to add Postgres MCP Lite.
The location of this file depends on your operating system:
- MacOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%/Claude/claude_desktop_config.json`

You can also use `Settings` menu item in Claude Desktop to locate the configuration file.

You will now edit the `mcpServers` section of the configuration file.

##### If you are using `pipx`

```json
{
  "mcpServers": {
    "postgres": {
      "command": "pg-mcp",
      "args": [
        "--access-mode=unrestricted"
      ],
      "env": {
        "DATABASE_URI": "postgresql://username:password@localhost:5432/dbname"
      }
    }
  }
}
```


##### If you are using `uv`

```json
{
  "mcpServers": {
    "postgres": {
      "command": "uv",
      "args": [
        "run",
        "pg-mcp",
        "--access-mode=unrestricted"
      ],
      "env": {
        "DATABASE_URI": "postgresql://username:password@localhost:5432/dbname"
      }
    }
  }
}
```


##### Connection URI

Replace `postgresql://...` with your [Postgres database connection URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS).


##### Multiple Database Connections

Postgres MCP Lite supports connecting to multiple databases simultaneously. This is useful when you need to work across different databases (e.g., application database, ETL database, analytics database).

To configure multiple connections, define additional environment variables with the pattern `DATABASE_URI_<NAME>`:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "pg-mcp",
      "args": ["--access-mode=unrestricted"],
      "env": {
        "DATABASE_URI_APP": "postgresql://user:pass@localhost:5432/app_db",
        "DATABASE_URI_ETL": "postgresql://user:pass@localhost:5432/etl_db",
        "DATABASE_URI_ANALYTICS": "postgresql://user:pass@localhost:5432/analytics_db",
        "DATABASE_DESC_APP": "Main application database with user data and transactions",
        "DATABASE_DESC_ETL": "ETL staging database for data processing pipelines",
        "DATABASE_DESC_ANALYTICS": "Read-only analytics database with aggregated metrics"
      }
    }
  }
}
```

Each connection is identified by its name (the part after `DATABASE_URI_`, converted to lowercase):
- `DATABASE_URI_APP` → connection name: `"app"`
- `DATABASE_URI_ETL` → connection name: `"etl"`
- `DATABASE_URI_ANALYTICS` → connection name: `"analytics"`

**Connection Descriptions**: You can optionally provide descriptions for each connection using `DATABASE_DESC_<NAME>` environment variables. These descriptions help the AI assistant understand which database to use for different tasks. The descriptions are:
- Automatically displayed in the server context (visible to the AI without requiring a tool call)
- Useful for guiding the AI to select the appropriate database

When using tools, the LLM will specify which connection to use via the `conn_name` parameter:
- `list_schemas(conn_name="app")` - Lists schemas in the app database
- `explain_query(conn_name="etl", sql="SELECT ...")` - Explains query in the ETL database

For backward compatibility, `DATABASE_URI` (without a suffix) maps to the connection name `"default"`.


##### Access Mode

Postgres MCP Lite supports multiple *access modes* to give you control over the operations that the AI agent can perform on the database:
- **Unrestricted Mode**: Allows full read/write access to modify data and schema. It is suitable for development environments.
- **Restricted Mode**: Limits operations to read-only transactions and imposes constraints on resource utilization (presently only execution time). It is suitable for production environments.

To use restricted mode, replace `--access-mode=unrestricted` with `--access-mode=restricted` in the configuration examples above.


#### Claude Code Configuration

[Claude Code](https://docs.claude.com/en/docs/claude-code/overview) is Anthropic's agentic coding tool for your terminal. To configure Postgres MCP Lite with Claude Code:

1. **Install Claude Code** (if you haven't already):
   ```bash
   npm install -g @anthropic-ai/claude-code
   ```

2. **Edit your Claude Code configuration file**:
   - Location: `~/.claude.json` (Linux/macOS) or `%USERPROFILE%\.claude.json` (Windows)
   - Or use the CLI wizard: `claude mcp add`

3. **Add Postgres MCP Lite to your configuration**:

   ```json
   {
     "mcpServers": {
       "postgres": {
         "command": "pg-mcp",
         "args": ["--access-mode=unrestricted"],
         "env": {
           "DATABASE_URI": "postgresql://username:password@localhost:5432/dbname"
         }
       }
     }
   }
   ```

4. **Restart Claude Code** for changes to take effect. Verify with:
   ```bash
   claude mcp list
   ```

#### Other MCP Clients

Many MCP clients have similar configuration files to Claude Desktop, and you can adapt the examples above to work with the client of your choice.

- If you are using Cursor, you can use navigate from the `Command Palette` to `Cursor Settings`, then open the `MCP` tab to access the configuration file.
- If you are using Windsurf, you can navigate to from the `Command Palette` to `Open Windsurf Settings Page` to access the configuration file.
- If you are using Goose run `goose configure`, then select `Add Extension`.

## SSE Transport

Postgres MCP Lite supports the [SSE transport](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse), which allows multiple MCP clients to share one server, possibly a remote server.
To use the SSE transport, you need to start the server with the `--transport=sse` option.

For example, run:

```bash
DATABASE_URI=postgresql://username:password@localhost:5432/dbname \
  pg-mcp --access-mode=unrestricted --transport=sse
```

Then update your MCP client configuration to call the MCP server.
For example, in Cursor's `mcp.json` or Cline's `cline_mcp_settings.json` you can put:

```json
{
    "mcpServers": {
        "postgres": {
            "type": "sse",
            "url": "http://localhost:8000/sse"
        }
    }
}
```

For Windsurf, the format in `mcp_config.json` is slightly different:

```json
{
    "mcpServers": {
        "postgres": {
            "type": "sse",
            "serverUrl": "http://localhost:8000/sse"
        }
    }
}
```

## Usage Examples

### Explore Database Schema

Ask:
> Show me all the tables in the database and their structure.

### Generate SQL Queries

Ask:
> Write a query to find all orders from the past month with their customer details.

### Analyze Table Structure

Ask:
> What indexes exist on the orders table and what columns do they cover?

### Execute Data Queries

Ask:
> Show me the top 10 customers by order count in 2024.

## MCP Server API

The [MCP standard](https://modelcontextprotocol.io/) defines various types of endpoints: Tools, Resources, Prompts, and others.

Postgres MCP Lite provides functionality via [MCP tools](https://modelcontextprotocol.io/docs/concepts/tools) alone.
We chose this approach because the [MCP client ecosystem](https://modelcontextprotocol.io/clients) has widespread support for MCP tools.
This contrasts with the approach of other Postgres MCP servers, including the [Reference Postgres MCP Server](https://github.com/modelcontextprotocol/servers/tree/main/src/postgres), which use [MCP resources](https://modelcontextprotocol.io/docs/concepts/resources) to expose schema information.


Postgres MCP Lite provides 4 essential tools:

| Tool Name | Description |
|-----------|-------------|
| `list_schemas` | Lists all database schemas available in the PostgreSQL instance. |
| `list_objects` | Lists database objects (tables, views, sequences, extensions) within a specified schema. |
| `get_object_details` | Provides detailed information about a specific database object, including columns, constraints, and indexes. |
| `execute_sql` | Executes SQL statements on the database, with read-only limitations when connected in restricted mode. |


## Related Projects

**Other Postgres MCP Servers**
- [Reference PostgreSQL MCP Server](https://github.com/modelcontextprotocol/servers/tree/main/src/postgres) - Official reference implementation
- [PG-MCP](https://github.com/stuzero/pg-mcp-server) - Feature-rich PostgreSQL MCP server
- [Supabase Postgres MCP Server](https://github.com/supabase-community/supabase-mcp) - Supabase integration
- [Query MCP](https://github.com/alexander-zuev/supabase-mcp-server) - Three-tier safety architecture

## Technical Notes

### Postgres Client Library

Postgres MCP Lite uses [psycopg3](https://www.psycopg.org/) for asynchronous database connectivity. It leverages [libpq](https://www.postgresql.org/docs/current/libpq.html) for full Postgres feature support.

### Protected SQL Execution

Postgres MCP Lite provides two access modes:

- **Unrestricted Mode**: Full read/write access, suitable for development environments
- **Restricted Mode**: Read-only transactions with execution time limits, suitable for production

In restricted mode, SQL is parsed using [pglast](https://pglast.readthedocs.io/) to prevent transaction control statements that could circumvent read-only protections. All queries execute within read-only transactions and are automatically rolled back.

### Schema Information

Schema tools provide AI agents with the information needed to generate correct SQL. While LLMs can query Postgres system catalogs directly, dedicated tools ensure consistent, reliable schema exploration across different LLM capabilities.


## Postgres MCP Lite Development

The instructions below are for developers who want to work on Postgres MCP Lite, or users who prefer to install Postgres MCP Lite from source.

### Local Development Setup

1. **Install uv**:

   ```bash
   curl -sSL https://astral.sh/uv/install.sh | sh
   ```

2. **Clone the repository**:

   ```bash
   git clone https://github.com/andre-c-andersen/pg-mcp.git
   cd pg-mcp
   ```

3. **Install dependencies**:

   ```bash
   uv pip install -e .
   uv sync
   ```

4. **Run the server**:
   ```bash
   uv run pg-mcp "postgres://user:password@localhost:5432/dbname"
   ```
