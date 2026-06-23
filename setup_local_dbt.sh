#!/usr/bin/env bash
#
# One-time local setup for the Mazuria dbt project + dbt MCP server.
# Creates a venv, installs dbt-postgres + dbt-mcp, loads env vars from .env,
# then runs `dbt debug` / `dbt deps` to verify the connection.
#
# Re-runnable: safe to run again to refresh deps.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Determine which Python command to use
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Error: Neither python3 nor python found. Please install Python."
    exit 1
fi

echo "###############################"
echo ""
echo "Creating virtual environment with $PYTHON_CMD..."
$PYTHON_CMD -m venv env
source env/bin/activate
echo ""

echo "###############################"
echo ""
echo "Installing dbt-core, dbt-postgres..."
python -m pip install --upgrade pip >/dev/null 2>&1
python -m pip install dbt-core==1.9.8 dbt-postgres==1.9.0
echo ""

echo "###############################"
echo ""
echo "Loading environment from .env..."
# Local profile dir (overrides the VM path baked into .env).
set -a
source .env 2>/dev/null || true
set +a
export DBT_PROFILES_DIR="$HERE/profile"
echo "DBT_PROFILES_DIR=$DBT_PROFILES_DIR"
echo ""

echo "###############################"
echo ""
echo "Verifying connection and installing packages..."
dbt debug
dbt clean
dbt deps
echo ""
echo "✓ Local dbt setup complete. dbt MCP launcher: ./run_dbt_mcp.sh"
