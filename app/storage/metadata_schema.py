from __future__ import annotations

METADATA_TABLES: tuple[str, ...] = (
    "ops_run_manifest",
    "fact_job_run",
    "fact_job_step_run",
    "fact_pipeline_dependency_state",
    "fact_health_snapshot",
    "fact_disk_watermark_event",
    "fact_retention_cleanup_run",
    "fact_alert_event",
    "fact_recovery_action",
    "fact_active_ops_policy",
    "fact_active_lock",
    "fact_latest_app_snapshot",
    "fact_latest_report_index",
    "fact_release_candidate_check",
    "fact_ui_data_freshness_snapshot",
)


def postgres_metadata_ddl(schema: str) -> tuple[str, ...]:
    qualified = lambda table_name: f'"{schema}"."{table_name}"'
    return (
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("ops_run_manifest")} (
            run_id TEXT PRIMARY KEY,
            run_type TEXT NOT NULL,
            as_of_date DATE,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            status TEXT NOT NULL,
            input_sources_json TEXT,
            output_artifacts_json TEXT,
            model_version TEXT,
            feature_version TEXT,
            ranking_version TEXT,
            git_commit TEXT,
            notes TEXT,
            error_message TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_job_run")} (
            run_id TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            status TEXT NOT NULL,
            as_of_date DATE,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            root_run_id TEXT NOT NULL,
            parent_run_id TEXT,
            recovery_of_run_id TEXT,
            lock_name TEXT,
            policy_id TEXT,
            policy_version TEXT,
            dry_run BOOLEAN NOT NULL,
            step_count INTEGER NOT NULL,
            failed_step_count INTEGER NOT NULL,
            artifact_count INTEGER NOT NULL,
            notes TEXT,
            error_message TEXT,
            details_json TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_job_run_job_name_started_at
        ON {qualified("fact_job_run")} (job_name, started_at DESC)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_job_run_status_started_at
        ON {qualified("fact_job_run")} (status, started_at DESC)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_job_step_run")} (
            step_run_id TEXT PRIMARY KEY,
            job_run_id TEXT NOT NULL,
            step_name TEXT NOT NULL,
            step_order INTEGER NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            critical_flag BOOLEAN NOT NULL,
            notes TEXT,
            error_message TEXT,
            details_json TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_job_step_run_job_run_id
        ON {qualified("fact_job_step_run")} (job_run_id, step_order)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_pipeline_dependency_state")} (
            checked_at TIMESTAMPTZ NOT NULL,
            pipeline_name TEXT NOT NULL,
            dependency_name TEXT NOT NULL,
            status TEXT NOT NULL,
            ready_flag BOOLEAN NOT NULL,
            required_state TEXT,
            observed_state TEXT,
            as_of_date DATE,
            details_json TEXT,
            job_run_id TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (checked_at, pipeline_name, dependency_name)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_health_snapshot")} (
            snapshot_at TIMESTAMPTZ NOT NULL,
            health_scope TEXT NOT NULL,
            component_name TEXT NOT NULL,
            status TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value_double DOUBLE PRECISION,
            metric_value_text TEXT,
            as_of_date DATE,
            details_json TEXT,
            job_run_id TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (snapshot_at, health_scope, component_name, metric_name)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_disk_watermark_event")} (
            event_id TEXT PRIMARY KEY,
            measured_at TIMESTAMPTZ NOT NULL,
            disk_status TEXT NOT NULL,
            usage_ratio DOUBLE PRECISION NOT NULL,
            used_gb DOUBLE PRECISION NOT NULL,
            available_gb DOUBLE PRECISION NOT NULL,
            total_gb DOUBLE PRECISION NOT NULL,
            policy_id TEXT,
            policy_version TEXT,
            cleanup_required_flag BOOLEAN NOT NULL,
            emergency_block_flag BOOLEAN NOT NULL,
            notes TEXT,
            details_json TEXT,
            job_run_id TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_retention_cleanup_run")} (
            cleanup_run_id TEXT PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL,
            dry_run BOOLEAN NOT NULL,
            cleanup_scope TEXT NOT NULL,
            removed_file_count BIGINT NOT NULL,
            reclaimed_bytes BIGINT NOT NULL,
            target_paths_json TEXT,
            notes TEXT,
            details_json TEXT,
            job_run_id TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_alert_event")} (
            alert_id TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            component_name TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT NOT NULL,
            details_json TEXT,
            job_run_id TEXT,
            resolved_at TIMESTAMPTZ
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_recovery_action")} (
            recovery_action_id TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            action_type TEXT NOT NULL,
            status TEXT NOT NULL,
            target_job_run_id TEXT,
            triggered_by_run_id TEXT,
            recovery_run_id TEXT,
            lock_name TEXT,
            notes TEXT,
            details_json TEXT,
            finished_at TIMESTAMPTZ
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_active_ops_policy")} (
            ops_policy_registry_id TEXT PRIMARY KEY,
            policy_id TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            policy_name TEXT NOT NULL,
            policy_path TEXT NOT NULL,
            effective_from_at TIMESTAMPTZ NOT NULL,
            effective_to_at TIMESTAMPTZ,
            active_flag BOOLEAN NOT NULL,
            promotion_type TEXT NOT NULL,
            note TEXT,
            rollback_of_registry_id TEXT,
            config_json TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_active_lock")} (
            lock_name TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            owner_run_id TEXT NOT NULL,
            acquired_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            released_at TIMESTAMPTZ,
            release_reason TEXT,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_latest_app_snapshot")} (
            snapshot_id TEXT PRIMARY KEY,
            snapshot_ts TIMESTAMPTZ NOT NULL,
            as_of_date DATE,
            latest_daily_bundle_run_id TEXT,
            latest_daily_bundle_status TEXT,
            latest_evaluation_date DATE,
            latest_evaluation_run_id TEXT,
            latest_intraday_session_date DATE,
            latest_intraday_run_id TEXT,
            latest_portfolio_as_of_date DATE,
            latest_portfolio_run_id TEXT,
            active_intraday_policy_id TEXT,
            active_meta_model_ids_json TEXT,
            active_portfolio_policy_id TEXT,
            active_ops_policy_id TEXT,
            health_status TEXT,
            market_regime_family TEXT,
            top_actionable_symbol_list_json TEXT,
            latest_report_bundle_id TEXT,
            critical_alert_count BIGINT NOT NULL DEFAULT 0,
            warning_alert_count BIGINT NOT NULL DEFAULT 0,
            notes TEXT,
            details_json TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_latest_report_index")} (
            report_index_id TEXT PRIMARY KEY,
            report_type TEXT NOT NULL,
            report_key TEXT NOT NULL,
            as_of_date DATE,
            generated_ts TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL,
            run_id TEXT,
            artifact_path TEXT NOT NULL,
            artifact_format TEXT NOT NULL,
            published_flag BOOLEAN NOT NULL DEFAULT FALSE,
            dry_run_flag BOOLEAN NOT NULL DEFAULT FALSE,
            summary_json TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_release_candidate_check")} (
            release_candidate_check_id TEXT PRIMARY KEY,
            check_ts TIMESTAMPTZ NOT NULL,
            environment TEXT NOT NULL,
            check_name TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            detail_json TEXT,
            recommended_action TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qualified("fact_ui_data_freshness_snapshot")} (
            freshness_snapshot_id TEXT PRIMARY KEY,
            snapshot_ts TIMESTAMPTZ NOT NULL,
            page_name TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            latest_available_ts TIMESTAMPTZ,
            freshness_seconds DOUBLE PRECISION,
            stale_flag BOOLEAN NOT NULL,
            warning_level TEXT NOT NULL,
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE OR REPLACE VIEW "{schema}"."vw_latest_pipeline_dependency_state" AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY pipeline_name, dependency_name
                    ORDER BY checked_at DESC, created_at DESC
                ) AS row_number
            FROM {qualified("fact_pipeline_dependency_state")}
        ) ranked
        WHERE row_number = 1
        """,
        f"""
        CREATE OR REPLACE VIEW "{schema}"."vw_latest_health_snapshot" AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY health_scope, component_name, metric_name
                    ORDER BY snapshot_at DESC, created_at DESC
                ) AS row_number
            FROM {qualified("fact_health_snapshot")}
        ) ranked
        WHERE row_number = 1
        """,
        f"""
        CREATE OR REPLACE VIEW "{schema}"."vw_latest_app_snapshot" AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    ORDER BY snapshot_ts DESC, created_at DESC
                ) AS row_number
            FROM {qualified("fact_latest_app_snapshot")}
        ) ranked
        WHERE row_number = 1
        """,
        f"""
        CREATE OR REPLACE VIEW "{schema}"."vw_latest_report_index" AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY report_type
                    ORDER BY generated_ts DESC, created_at DESC
                ) AS row_number
            FROM {qualified("fact_latest_report_index")}
        ) ranked
        WHERE row_number = 1
        """,
        f"""
        CREATE OR REPLACE VIEW "{schema}"."vw_latest_release_candidate_check" AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY check_name
                    ORDER BY check_ts DESC, created_at DESC
                ) AS row_number
            FROM {qualified("fact_release_candidate_check")}
        ) ranked
        WHERE row_number = 1
        """,
        f"""
        CREATE OR REPLACE VIEW "{schema}"."vw_latest_ui_data_freshness_snapshot" AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY page_name, dataset_name
                    ORDER BY snapshot_ts DESC, created_at DESC
                ) AS row_number
            FROM {qualified("fact_ui_data_freshness_snapshot")}
        ) ranked
        WHERE row_number = 1
        """,
    )
