#!/usr/bin/env bash
#
# Canonical dbt-core build for the Mazuria project. Used by Airflow (the
# mazuria_etl DAG SSHes to the Airflow VM and runs this) and runnable by hand.
#
# - Relocatable (works from whatever directory it lives in).
# - Bootstraps the venv + dbt on first run via setup_local_dbt.sh.
# - Loads DB creds from .env and uses the committed profile (profile/).
# - Builds with --target prod by default (-> dbt_prod_* schemas); override with
#   DBT_TARGET=dev for a sandbox build into dbt_dev_oleksandr.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# On the VM the checkout always tracks main; pull latest. On a feature branch
# (local dev) skip the pull so we don't merge main into the branch.
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
if [ "$branch" = "main" ]; then
    git pull --ff-only origin main
fi

# Bootstrap the environment on first run (creates env/, installs dbt, deps).
# if [ ! -x "env/bin/dbt" ]; then
#     echo "venv missing — bootstrapping via setup_local_dbt.sh..."
    ./setup_local_dbt.sh
# fi

source env/bin/activate

# Load DB credentials and point dbt at the committed profile.
set -a
source "$HERE/.env" 2>/dev/null || true
set +a
export DBT_PROFILES_DIR="$HERE/profile"

dbt deps
dbt build --target "${DBT_TARGET:-prod}"
