# ruff: noqa: E501

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RunbookCase:
    case_id: str
    symptom: str
    detection_hint: str
    immediate_action: str
    follow_up: str


RUNBOOK_CASES: tuple[RunbookCase, ...] = (
    RunbookCase(
        case_id="LATEST_UNIQUENESS_DRIFT",
        symptom="latest view가 1개 canonical row를 보장하지 못함",
        detection_hint="validate_latest_layer_consistency / duplicate latest groups > 0",
        immediate_action="latest writer 스크립트를 순차 재실행하고 unique grain이 맞는지 점검한다.",
        follow_up="PK 또는 view partition 기준을 계약 문서와 일치시킨다.",
    ),
    RunbookCase(
        case_id="ARTIFACT_REFERENCE_BROKEN",
        symptom="report index가 가리키는 preview/payload 파일이 없음",
        detection_hint="validate_artifact_reference_integrity FAIL",
        immediate_action="해당 report renderer를 다시 실행해서 preview/payload를 복구한다.",
        follow_up="cleanup allowlist/protected 경로와 report index writer를 함께 점검한다.",
    ),
    RunbookCase(
        case_id="WEEKEND_FRESHNESS_MISCLASSIFIED",
        symptom="주말/휴장일에 최신 trading-day 데이터가 CRITICAL로 표시됨",
        detection_hint="latest:freshness:weekend_holiday_classification FAIL",
        immediate_action="dim_trading_calendar와 freshness snapshot builder를 다시 실행한다.",
        follow_up="거래일 기준 freshness rule과 pre/post-close cutoff를 문서와 코드 양쪽에서 유지한다.",
    ),
    RunbookCase(
        case_id="FUTURE_EFFECTIVE_ACTIVE_ID",
        symptom="snapshot에 future-effective active policy/model id가 노출됨",
        detection_hint="active_*_consistency FAIL",
        immediate_action="latest snapshot/materializer를 재실행하고 effective_from/effective_to filter를 점검한다.",
        follow_up="active registry rollback/freeze가 overlap 없이 기록되는지 함께 검증한다.",
    ),
    RunbookCase(
        case_id="REPRESENTATIVE_DUPLICATES",
        symptom="대표 fact/dim 테이블에서 duplicate key group이 탐지됨",
        detection_hint="validate_db_contracts FAIL with duplicate_row_groups > 0",
        immediate_action="해당 rerun job을 중지하고 primary grain에 맞는 upsert/delete 범위를 재검토한다.",
        follow_up="contract matrix에 rerun rule과 lineage를 보강하고 회귀 테스트를 추가한다.",
    ),
    RunbookCase(
        case_id="LATEST_ARTIFACT_NOT_PROTECTED",
        symptom="latest report artifact가 retention cleanup 보호대상에 포함되지 않음",
        detection_hint="artifact cleanup_safe WARN/FAIL",
        immediate_action="cleanup dry-run으로 영향 범위를 확인하고 protected path 계산을 재검토한다.",
        follow_up="latest report index와 retention allowlist/protected 규칙을 함께 유지한다.",
    ),
)
