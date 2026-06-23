#!/usr/bin/env bash
#
# Launcher for the local dbt MCP server (invoked by ../.mcp.json).
# Loads credentials from .env so no secrets live in .mcp.json, points the
# server at the local Postgres profile, and disables the platform-only tools
# (semantic layer / discovery / remote) since this is a dbt-core setup.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Install the dbt MCP server into the project venv on first use (kept out of
# setup_local_dbt.sh so the Airflow VM build stays lean).
if [ ! -x "$HERE/env/bin/dbt-mcp" ]; then
    "$HERE/env/bin/pip" install dbt-mcp >/dev/null
fi

# Load env vars from .env (PG_HOST, PG_USER, PG_PASSWORD, ...).
set -a
source "$HERE/.env" 2>/dev/null || true
set +a

# Local-only dbt MCP configuration.
export DBT_PROJECT_DIR="$HERE"
export DBT_PROFILES_DIR="$HERE/profile"
export DBT_PATH="$HERE/env/bin/dbt"
export DISABLE_SEMANTIC_LAYER="true"
export DISABLE_DISCOVERY="true"
export DISABLE_REMOTE="true"
export DISABLE_SQL="true"
export DISABLE_ADMIN_API="true"

exec "$HERE/env/bin/dbt-mcp"
