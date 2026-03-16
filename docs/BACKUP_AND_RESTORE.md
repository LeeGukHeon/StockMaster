# Backup And Restore

Backup entrypoint:

- `bash scripts/server/backup_server_data.sh`

Restore checklist:

1. Stop app and scheduler services.
2. Restore runtime data into `/opt/stockmaster/runtime`.
3. Restore backups or database snapshots as needed.
4. Restart the stack and run smoke checks.

Use `docs/RUNBOOK_SERVER_OPERATIONS.md` for the full operational sequence.
