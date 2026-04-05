# mazuria_dbt

dbt project transforming raw data from Google Sheets, Google Calendar, and Pipefy into analytics tables in PostgreSQL.

## Stack

- dbt v1.2.1, PostgreSQL
- Packages: `dbt_date` (v0.10.0), `dbt_utils` (v1.1.1)

## Model Layers

```
staging/       stg_   — source-aligned, one-to-one with raw tables
intermediate/  int_   — business logic, joins, transformations
marts/         fct_ / dim_ / mrt_  — business-facing analytics tables
workspaces/    — domain-specific sub-projects (e.g. sf_workload)
```

## Naming Conventions

- `stg_[source]_[table]` — staging models
- `int_[business_process]` — intermediate models
- `fct_[entity]` — fact tables
- `dim_[entity]` — dimension tables

## Schema Naming (via `generate_schema_name` macro)

| Environment | Schema prefix |
|-------------|--------------|
| prod        | `dbt_prod_[schema]` |
| uat         | `dbt_uat_[schema]` |
| dev (oleksandrmazur) | `embucket` |
| CI          | `dbt_prod_ci` / `dbt_uat_ci` |

## Sources

- Database: `mazuria`, schema: `mazuria_raw`
- Always reference via `{{ source() }}` — never query raw tables directly

## Materialization

All models default to `table` materialization (set in `dbt_project.yml`).

## Common Commands

```bash
dbt deps              # install packages
dbt run               # run all models
dbt test              # run data quality tests
dbt run --select staging   # run only staging layer
dbt run --select marts     # run only marts
dbt docs generate     # generate docs (auto-deployed to GitHub Pages on push to main)
```

## Environment Variables (never in repo)

`PG_HOST`, `PG_USER`, `PG_PASSWORD`, `PG_PORT`, `PG_DATABASE`

## DBT modeling rules project related

dbt Labs published their conventions. Here's what actually matters:

➢ Prefix tells you the layer
base_ → base (used for unions or pre-staging)
stg_ → staging (raw, source-conformed)
int_ → intermediate (business logic in progress)
dim_,fct_ → dimensions and facts in marts


➢ Double underscore separates source from entity
stg_stripe__payments, stg_shopify__orders
The __ is intentional. Single underscore is part of the name. Double underscore is a delimiter. One glance and you know where the data comes from.

➢ Folders mirror your naming
Staging models separated by the source, e.g /staging/stripe, /staging/shopify
Marts and ints are separateed by the domain: /marts/finance, /marts/marketing

➢ One staging model per source table
No exceptions. stg_stripe__payments is the single entry point for that table into your project.

➢ Primary keys follow object_id
account_id, order_id, customer_id — not just id. Always strings, not integers. It makes joins safer across systems.

## dbt rules in overall
Fields and model names
👥 Models should be pluralized, for example, customers, orders, products.
🔑 Each model should have a primary key.
🔑 The primary key of a model should be named <object>_id, for example, account_id. This makes it easier to know what id is being referenced in downstream joined models.
Use underscores for naming dbt models; avoid dots.
✅ models_without_dots
❌ models.with.dots
Most data platforms use dots to separate database.schema.object, so using underscores instead of dots reduces your need for quoting as well as the risk of issues in certain parts of dbt. For more background, refer to this GitHub issue.
🔑 Keys should be string data types.
🔑 Consistency is key! Use the same field names across models where possible. For example, a key to the customers table should be named customer_id rather than user_id or 'id'.
❌ Do not use abbreviations or aliases. Emphasize readability over brevity. For example, do not use cust for customer or o for orders.
❌ Avoid reserved words as column names.
➕ Booleans should be prefixed with is_ or has_.
🕰️ Timestamp columns should be named <event>_at(for example, created_at) and should be in UTC. If a different timezone is used, this should be indicated with a suffix (created_at_pt).
📆 Dates should be named <event>_date. For example, created_date.
🔙 Events dates and times should be past tense — created, updated, or deleted.
💱 Price/revenue fields should be in decimal currency (19.99 for $19.99; many app databases store prices as integers in cents). If a non-decimal currency is used, indicate this with a suffix (price_in_cents).
🐍 Schema, table and column names should be in snake_case.
🏦 Use names based on the business terminology, rather than the source terminology. For example, if the source database uses user_id but the business calls them customer_id, use customer_id in the model.
🔢 Versions of models should use the suffix _v1, _v2, etc for consistency (customers_v1 and customers_v2).
🗄️ Use a consistent ordering of data types and consider grouping and labeling columns by type, as in the example below. This will minimize join errors and make it easier to read the model, as well as help downstream consumers of the data understand the data types and scan models for the columns they need. We prefer to use the following order: ids, strings, numerics, booleans, dates, and timestamps.