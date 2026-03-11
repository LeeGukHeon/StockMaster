# DEPLOY OCI Metadata Update

## Scope

This note supplements `docs/DEPLOY_OCI.md` after the metadata-store split and host scheduler worker transition.

## Current Server Layout

- code checkout:
  - `/opt/stockmaster/app`
- runtime root:
  - `/opt/stockmaster/runtime`
- analytics DB:
  - `/opt/stockmaster/runtime/data/marts/main.duckdb`
- metadata DB:
  - docker service `metadata_db`
  - loopback port `127.0.0.1:5433`

## Required `.env.server` Keys

When metadata split is enabled, the server env must contain:

- `METADATA_DB_ENABLED=true`
- `METADATA_DB_BACKEND=postgres`
- `METADATA_DB_SCHEMA=stockmaster_meta`
- `METADATA_DB_POSTGRES_DB=stockmaster_meta`
- `METADATA_DB_POSTGRES_USER=stockmaster`
- `METADATA_DB_POSTGRES_PASSWORD=...`
- `METADATA_DB_HOST_PORT=5433`
- `METADATA_DB_URL=postgresql://stockmaster:...@metadata_db:5432/stockmaster_meta`

## Startup Flow

The current startup flow is:

1. build the application image when needed
2. start `metadata_db` first when metadata split is enabled
3. wait for postgres readiness
4. run `scripts/bootstrap_metadata_store.py`
5. run `scripts/bootstrap.py`
6. start `app` and `nginx`
7. run local smoke checks

## Validation Commands

```bash
cd /opt/stockmaster/app
FORCE_BUILD=true bash scripts/server/start_server.sh
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps
```

Check metadata DB rows:

```bash
cd /opt/stockmaster/app
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml \
  exec -T metadata_db psql -U stockmaster -d stockmaster_meta
```

## Notes

- local development still works with `METADATA_DB_ENABLED=false`
- server metadata split does not require re-downloading historical market data
- the existing DuckDB analytics data remains reusable
