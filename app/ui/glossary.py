from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GlossaryEntry:
    term: str
    short_label: str
    definition: str


GLOSSARY_ENTRIES: tuple[GlossaryEntry, ...] = (
    GlossaryEntry(
        "Selection v2",
        "Selection v2",
        "ML 알파, 수급, 리스크, 구현 페널티를 결합한 현재 주력 종목 선별 엔진입니다.",
    ),
    GlossaryEntry(
        "Expected Alpha",
        "예상 알파",
        "동일 시장 equal-weight 기준 대비 기대하는 초과수익의 방향성과 크기를 뜻합니다.",
    ),
    GlossaryEntry(
        "Uncertainty",
        "불확실성",
        "예측 오차가 커질 가능성을 보수적으로 반영한 proxy 점수입니다.",
    ),
    GlossaryEntry(
        "Disagreement",
        "모델 불일치",
        "앙상블 member 예측이 서로 얼마나 벌어지는지를 요약한 proxy 점수입니다.",
    ),
    GlossaryEntry(
        "Implementation Penalty",
        "구현 페널티",
        "유동성, 스프레드, 체결 불리함 같은 실행 마찰을 반영한 차감 항목입니다.",
    ),
    GlossaryEntry(
        "Flow Score",
        "수급 점수",
        "외국인·기관·개인 수급 방향과 강도를 요약한 보조 점수입니다.",
    ),
    GlossaryEntry(
        "Timing Assisted",
        "타이밍 보조",
        "신규 진입과 추가 매수에만 장중 timing layer를 적용하는 실행 모드입니다.",
    ),
    GlossaryEntry(
        "Stale",
        "지연 상태",
        "현재 시점 대비 마지막 materialized output이 오래되어 신선도가 낮은 상태입니다.",
    ),
    GlossaryEntry(
        "Degraded",
        "성능 저하",
        "시스템은 동작하지만 일부 데이터나 리포트가 부분적으로만 준비된 상태입니다.",
    ),
    GlossaryEntry(
        "Release Candidate",
        "릴리즈 후보",
        "배포 직전 사용자 워크플로우, 리포트, 네비게이션, freshness를 점검한 상태를 뜻합니다.",
    ),
)


def glossary_mapping() -> dict[str, GlossaryEntry]:
    return {entry.term: entry for entry in GLOSSARY_ENTRIES}


def resolve_glossary(term: str) -> GlossaryEntry | None:
    return glossary_mapping().get(term)


def glossary_markdown() -> str:
    lines = ["# 용어집", ""]
    for entry in GLOSSARY_ENTRIES:
        lines.append(f"## {entry.term}")
        lines.append(entry.definition)
        lines.append("")
    return "\n".join(lines).strip()
