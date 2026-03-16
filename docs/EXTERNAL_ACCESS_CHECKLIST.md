# External Access Checklist

Confirm the following before opening the service publicly:

1. Nginx is serving on port `80` and Streamlit stays internal.
2. Dashboard access protection is enabled when required.
3. OCI security list / NSG rules allow only intended inbound ports.
4. `bash scripts/server/check_public_access.sh` passes from the server.
5. `bash scripts/server/smoke_test_server.sh` passes after deployment.

Operational details live in `docs/RUNBOOK_SERVER_OPERATIONS.md`.
