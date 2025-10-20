<div align="center">

<img src="https://raw.githubusercontent.com/andre-c-andersen/pg-mcp/refs/heads/master/assets/postgres-mcp-lite.png" alt="Postgres MCP Lite Logo" width="600"/>

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![PyPI - Version](https://img.shields.io/pypi/v/pg-mcp)](https://pypi.org/project/pg-mcp/)
[![Twitter Follow](https://img.shields.io/twitter/follow/AndreCAndersen?style=flat)](https://x.com/AndreCAndersen)
[![Contributors](https://img.shields.io/github/contributors/andre-c-andersen/pg-mcp)](https://github.com/andre-c-andersen/pg-mcp/graphs/contributors)

<h3>A lightweight, read-only-by-default Postgres MCP server for data analysts and ETL developers.</h3>

</div>

## Overview

**Postgres MCP Lite** is a lightweight, open-source Model Context Protocol (MCP) server for PostgreSQL, designed specifically for **data analysts and ETL developers** who need to explore and understand database schemas and data.

This is a streamlined fork of [postgres-mcp](https://github.com/crystaldba/postgres-mcp) by [Crystal DBA](https://www.linkedin.com/company/crystaldba/), focused on **safe data exploration** rather than database administration:

- **🔍 Data Exploration** - Understand database schemas, table structures, and relationships with AI assistance
- **📊 Query Analysis** - Generate and execute SQL queries to analyze data patterns and relationships
- **🛡️ Read-Only by Default** - Defaults to restricted mode with SQL validation, preventing accidental modifications
- **🔌 Multiple [Transports](https://modelcontextprotocol.io/docs/concepts/transports)** - Supports both stdio and SSE
- **🌐 Multi-Database Support** - Connect to multiple databases simultaneously (app, ETL, analytics, etc.)

**Perfect for:**
- 📈 Data analysts exploring production databases safely
- 🔄 ETL developers understanding source and target schemas
- 🤖 AI-assisted data discovery and documentation
- 📝 Generating data analysis queries with LLM help

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
      "env": {
        "DATABASE_URI_STAGE_EXAMPLE": "postgresql://user:pass@localhost:5432/stage_db",
        "DATABASE_URI_DEV_EXAMPLE": "postgresql://user:pass@localhost:5432/dev_db",
        "DATABASE_DESC_STAGE_EXAMPLE": "Staging database for testing",
        "DATABASE_DESC_DEV_EXAMPLE": "Development database for local work"
      }
    }
  }
}
```

Each connection is identified by its name (the part after `DATABASE_URI_`, converted to lowercase):
- `DATABASE_URI_STAGE_EX` → connection name: `"stage_ex"`
- `DATABASE_URI_DEV_EX` → connection name: `"dev_ex"`

**Connection Descriptions**: You can optionally provide descriptions for each connection using `DATABASE_DESC_<NAME>` environment variables. These descriptions help the AI assistant understand which database to use for different tasks. The descriptions are:
- Automatically displayed in the server context (visible to the AI without requiring a tool call)
- Useful for guiding the AI to select the appropriate database

When using tools, the LLM will specify which connection to use via the `conn_name` parameter:
- `list_schemas(conn_name="stage_example")` - Lists schemas in the staging database
- `execute_sql(conn_name="dev_example", sql="SELECT ...")` - Executes query in the development database

For backward compatibility, `DATABASE_URI` (without a suffix) maps to the connection name `"default"`.


##### Access Mode

**Restricted mode (read-only) is the default.** You can query and explore schemas safely—writes and DDL are blocked.

**Allowed:** SELECT, EXPLAIN, SHOW, ANALYZE, schema introspection
**Blocked:** INSERT, UPDATE, DELETE, CREATE, ALTER, DROP

**💡 Best Practice:** Use database credentials with read-only permissions. Software makes mistakes—limit what an AI agent can access, just like you would for any automated process.

**⚠️ Data Privacy:** Database content will be sent to your LLM provider. Only connect to databases containing data you have the right to share. Consider data protection regulations (GDPR, HIPAA, etc.) and your organization's policies before use.

**⚠️ Unrestricted Mode (Dangerous)**

Only connect to databases you're willing to destroy. Allows full write access and DDL. Add `--access-mode=unrestricted` to the args array as shown in the `uv` example above.


##### Logging Configuration

When using stdio transport (the default), Postgres MCP Lite writes logs to stderr to avoid interfering with the MCP protocol (which uses stdout). When using SSE transport, logs go to stdout.

You can control the logging verbosity using the `LOG_LEVEL` environment variable:

- `DEBUG` - Show all logs including debug messages
- `INFO` - Show info, warning, and error messages (default)
- `WARNING` - Show only warnings and errors
- `ERROR` - Show only errors
- `CRITICAL` - Show only critical errors
- `NONE` - Disable all logging

Example configuration with logging disabled:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "pg-mcp",
      "env": {
        "DATABASE_URI": "postgresql://username:password@localhost:5432/dbname",
        "LOG_LEVEL": "NONE"
      }
    }
  }
}
```


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
  pg-mcp --transport=sse
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

### Understand Data Relationships

Ask:
> What columns link the orders and customers tables? Show me a sample join query.

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


## Technical Notes

### Postgres Client Library

Postgres MCP Lite uses [psycopg3](https://www.psycopg.org/) in **synchronous mode** for reliable database connectivity. It leverages [libpq](https://www.postgresql.org/docs/current/libpq.html) for full Postgres feature support.

### Safe SQL Execution by Default

Postgres MCP Lite **defaults to restricted mode** for safe data exploration:

- **Restricted Mode (Default)**: Read-only transactions with SQL validation and execution time limits. Perfect for production databases.
- **Unrestricted Mode**: Full read/write access for development or when explicitly needed.

In restricted mode, SQL is parsed using [pglast](https://pglast.readthedocs.io/) to validate that only safe, read-only operations are allowed (SELECT, EXPLAIN, SHOW, ANALYZE, VACUUM). All queries execute within read-only transactions and are automatically rolled back, preventing accidental data modifications.

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
