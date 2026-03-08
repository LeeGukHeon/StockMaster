from __future__ import annotations

from app.ui.glossary import GLOSSARY_ENTRIES, glossary_mapping, glossary_markdown


def test_glossary_contains_core_terms() -> None:
    mapping = glossary_mapping()
    expected_terms = {
        "Selection v2",
        "Expected Alpha",
        "Uncertainty",
        "Disagreement",
        "Implementation Penalty",
        "Flow Score",
        "Timing Assisted",
        "Stale",
        "Degraded",
        "Release Candidate",
    }
    assert expected_terms.issubset(mapping.keys())
    assert len(mapping) == len(GLOSSARY_ENTRIES)


def test_glossary_markdown_renders_headings() -> None:
    markdown = glossary_markdown()
    assert markdown.startswith("# 용어집")
    assert "## 선정 엔진 v2" in markdown
    assert "## 릴리스 점검" in markdown
