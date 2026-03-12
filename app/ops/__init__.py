from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "check_pipeline_dependencies": ("app.ops.health", "check_pipeline_dependencies"),
    "cleanup_disk_watermark": ("app.ops.maintenance", "cleanup_disk_watermark"),
    "enforce_retention_policies": ("app.ops.maintenance", "enforce_retention_policies"),
    "force_release_stale_lock": ("app.ops.maintenance", "force_release_stale_lock"),
    "freeze_active_ops_policy": ("app.ops.policy", "freeze_active_ops_policy"),
    "load_active_or_default_ops_policy": ("app.ops.policy", "load_active_or_default_ops_policy"),
    "materialize_health_snapshots": ("app.ops.health", "materialize_health_snapshots"),
    "publish_discord_ops_alerts": ("app.ops.report", "publish_discord_ops_alerts"),
    "reconcile_failed_runs": ("app.ops.maintenance", "reconcile_failed_runs"),
    "recover_incomplete_runs": ("app.ops.maintenance", "recover_incomplete_runs"),
    "render_ops_report": ("app.ops.report", "render_ops_report"),
    "rollback_active_ops_policy": ("app.ops.policy", "rollback_active_ops_policy"),
    "rotate_and_compress_logs": ("app.ops.maintenance", "rotate_and_compress_logs"),
    "run_daily_audit_lite_bundle": ("app.ops.bundles", "run_daily_audit_lite_bundle"),
    "run_daily_close_bundle": ("app.ops.bundles", "run_daily_close_bundle"),
    "run_docker_build_cache_cleanup_bundle": (
        "app.ops.bundles",
        "run_docker_build_cache_cleanup_bundle",
    ),
    "run_daily_evaluation_bundle": ("app.ops.bundles", "run_daily_evaluation_bundle"),
    "run_daily_post_close_bundle": ("app.ops.bundles", "run_daily_post_close_bundle"),
    "run_daily_research_pipeline": ("app.ops.bundles", "run_daily_research_pipeline"),
    "run_evaluation_bundle": ("app.ops.bundles", "run_evaluation_bundle"),
    "run_intraday_assist_bundle": ("app.ops.bundles", "run_intraday_assist_bundle"),
    "run_news_sync_bundle": ("app.ops.bundles", "run_news_sync_bundle"),
    "run_ops_maintenance_bundle": ("app.ops.bundles", "run_ops_maintenance_bundle"),
    "run_weekly_calibration_bundle": ("app.ops.bundles", "run_weekly_calibration_bundle"),
    "run_weekly_training_bundle": ("app.ops.bundles", "run_weekly_training_bundle"),
    "summarize_storage_usage": ("app.ops.maintenance", "summarize_storage_usage"),
    "validate_health_framework": ("app.ops.validation", "validate_health_framework"),
    "validate_ops_framework": ("app.ops.validation", "validate_ops_framework"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    module = import_module(module_name)
    value = getattr(module, attribute_name)
    globals()[name] = value
    return value
