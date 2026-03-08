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
        "선정 엔진 v2",
        "알파, 수급, 시장 국면 적합성, 불확실성, 모델 불일치, 실행 부담을 함께 반영해 "
        "종목을 고르는 현재 주력 선정 로직입니다.",
    ),
    GlossaryEntry(
        "Expected Alpha",
        "예상 알파",
        "같은 시장의 동일가중 기준 대비 기대하는 초과수익의 방향과 크기를 뜻합니다.",
    ),
    GlossaryEntry(
        "Uncertainty",
        "불확실성",
        "예측 신뢰도가 낮을수록 높아지는 보수적 지표입니다. 값이 높을수록 신뢰도는 낮다고 봅니다.",
    ),
    GlossaryEntry(
        "Disagreement",
        "모델 불일치",
        "앙상블을 이루는 개별 모델 예측이 서로 얼마나 벌어지는지를 요약한 지표입니다.",
    ),
    GlossaryEntry(
        "Implementation Penalty",
        "실행 패널티",
        "유동성 부족, 체결 부담, 스프레드 확장처럼 실제 운용 시 불리한 요소를 "
        "반영해 차감하는 항목입니다.",
    ),
    GlossaryEntry(
        "Flow Score",
        "수급 점수",
        "외국인, 기관, 개인의 순매수 흐름과 강도를 요약해 보여주는 보조 지표입니다.",
    ),
    GlossaryEntry(
        "Timing Assisted",
        "장중 보조 모드",
        "신규 진입과 추가 매수 시점에만 장중 판단 레이어를 적용하는 보수적 실행 방식입니다.",
    ),
    GlossaryEntry(
        "Stale",
        "지연 상태",
        "화면이 참조하는 최신 산출물이 기대 기준보다 오래돼 숫자와 링크를 "
        "보수적으로 봐야 하는 상태입니다.",
    ),
    GlossaryEntry(
        "Degraded",
        "저하 상태",
        "산출물은 존재하지만 품질, 완전성, 신선도 가운데 하나 이상이 기대 수준보다 "
        "낮은 상태입니다.",
    ),
    GlossaryEntry(
        "Release Candidate",
        "릴리스 점검",
        "배포 전 단계에서 페이지 연결, 리포트 산출물, 신선도, 문서, 경고 상태를 "
        "마지막으로 점검한 상태를 뜻합니다.",
    ),
)


def glossary_mapping() -> dict[str, GlossaryEntry]:
    return {entry.term: entry for entry in GLOSSARY_ENTRIES}


def resolve_glossary(term: str) -> GlossaryEntry | None:
    return glossary_mapping().get(term)


def glossary_markdown() -> str:
    lines = ["# 용어집", ""]
    for entry in GLOSSARY_ENTRIES:
        lines.append(f"## {entry.short_label}")
        lines.append(entry.definition)
        lines.append("")
    return "\n".join(lines).strip()
