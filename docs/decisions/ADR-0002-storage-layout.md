# ADR-0002: Storage Layout

## Status

Accepted

## Decision

Separate runtime storage into:

- `data/raw`
- `data/curated`
- `data/marts`
- `data/cache`
- `data/logs`
- `data/artifacts`

Persist operational metadata in DuckDB tables and keep generated runtime artifacts out of Git.

## Rationale

- The directory split matches the source-of-truth implementation spec.
- It keeps transient files, curated datasets, and analytical marts from being mixed together.
- `.gitignore` can safely exclude generated content while preserving the directory scaffold with `.gitkeep`.
