# ADR-0001: Foundation Stack

## Status

Accepted

## Decision

Use Python 3.11+, Streamlit, DuckDB, Parquet, YAML configuration, and `.env` overrides for the first implementation package.

## Rationale

- Python keeps provider integrations, batch scripts, and local operations simple.
- DuckDB + Parquet fits the single-user analytical workload and local-first development model.
- Streamlit is sufficient for a private dashboard in v1.
- YAML + `.env` keeps operational defaults versioned while allowing local secret overrides.
