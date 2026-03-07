# Codex 전달용 간단 브리프

아래 2개 문서를 먼저 읽고 구현을 시작하세요.

1. `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
2. `TICKET_000_Foundation_and_First_Work_Package.md`

핵심만 요약하면 다음과 같습니다.

- 프로젝트는 국내주식용 개인 리서치 플랫폼입니다.
- 자동매매가 아니라 장후 분석/랭킹/리포트/사후평가가 핵심입니다.
- 저장공간은 80GB 전제로 설계합니다.
- 공식 API 우선, 합법적 접근만 사용합니다.
- 실제 선별 엔진은 초과수익 예측 + 불확실성 + 비용 + 장세 적합성을 고려합니다.
- 사용자에게 보이는 점수는 설명용 UI 계층입니다.
- 이번 첫 작업은 기능 완성이 아니라 foundation 구축입니다.
- 반드시 run manifest, 설정 시스템, DuckDB bootstrap, provider skeleton, Streamlit skeleton을 먼저 만드세요.
- UI와 워커는 분리하세요.
- 전 종목 틱/호가 장기보관 구조를 먼저 만들지 마세요.

산출물은 실행 가능한 저장소 뼈대여야 하며, 이후 티켓에서 데이터 수집/피처/랭킹/리포트를 바로 붙일 수 있어야 합니다.

