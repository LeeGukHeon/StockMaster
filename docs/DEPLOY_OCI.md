# OCI Deployment

Use the server deployment bundle under `deploy/` and the operational runbook in
`docs/RUNBOOK_SERVER_OPERATIONS.md`.

Baseline flow:

1. Prepare `/opt/stockmaster/app`, `/opt/stockmaster/runtime`, and `/opt/stockmaster/backups`.
2. Copy `deploy/env/.env.server.example` to `deploy/env/.env.server` and fill secrets.
3. Start the stack with `bash scripts/server/start_server.sh`.
4. Install scheduler timers with `bash scripts/server/install_scheduler_units.sh`.
5. Validate public access and scheduler status from the runbook.
