# pm-recorder

Records Polymarket public market data on GitHub Actions, continuously: every hour a snapshot of
the markets ending within 45 days, the order books of a broad set of them and the taker trades
between snapshots; every minute the books and trades of the markets with the largest
liquidity-reward pools. Each run records ~6 hours, stores hourly gzipped JSON bundles, and starts
the next run before it ends.

Read-only public endpoints (Gamma, CLOB books, Data API v2); no trading.

The source of truth is a private repository; this copy is synced from it.

`wide.py` (workflow `wide`) is a second, lighter recorder: the reward pools ranked below the 60 above, a
sample every 5 minutes, hourly bundles as the run's artifact only.
