# Known Limitations

- StockMaster is not an order-routing or auto-trading system.
- DuckDB remains effectively single-writer. Heavy bundles are safer when executed sequentially.
- Some ML and intraday overlays are conservative because historical matured samples are still shallow.
- Probability bands and some uncertainty values are still proxy-style v1 implementations.
- Intraday and portfolio layers are downstream proposal layers and must not be interpreted as execution systems.
- Report center relies on artifact indexing; if report render scripts have not been run, some links will be missing by design.
- UI pages intentionally avoid heavy recomputation and prefer materialized outputs. Missing outputs are surfaced as warnings instead of being silently recomputed.
