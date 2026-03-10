# TICKET-006 — 정식 ML Alpha Model v1 + Uncertainty/Disagreement 기초 + Selection Engine v2

- 문서 목적: TICKET-005 이후, Codex가 바로 이어서 구현할 **정식 ML alpha 예측 계층 + 실제 uncertainty/disagreement 기초 + selection engine v2** 의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
  - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
  - `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
  - `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
  - TICKET-005 postmortem / evaluation / calibration diagnostic 완료
- 우선순위: 최상
- 기대 결과: **설명용 점수와 rule-based selection 을 넘어서, 재현 가능한 ML alpha 예측값·모델 기반 uncertainty·모델 간 disagreement 를 생성하고 이를 selection engine v2 에 반영할 수 있는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **StockMaster의 “설명형 연구 시스템”을 “실제 alpha 예측 엔진이 들어간 리서치 플랫폼”으로 올리는 첫 모델링 티켓**이다.

즉, 이번 티켓의 목표는 아래 일곱 가지를 안정적으로 만드는 것이다.

1. `fact_feature_snapshot` + `fact_forward_return_label` 을 사용해 **누수 없는 학습 데이터셋**을 만든다.
2. `D+1`, `D+5` 각각에 대해 **정식 ML alpha model v1** 을 학습한다.
3. 학습 결과를 **run / artifact / metric / prediction snapshot** 형태로 재현 가능하게 저장한다.
4. 실제 모델 출력 기반의 **point prediction, calibrated interval, uncertainty score** 를 만든다.
5. 복수 모델의 출력 차이를 사용해 **disagreement score** 를 만든다.
6. selection engine v1 위에 **selection engine v2** 를 올린다.
7. UI / Ops / Evaluation 에서 v1과 v2를 비교하고, fallback 조건까지 투명하게 보여준다.

이 티켓이 끝나면 다음 티켓에서는 장중 보조 엔진, 체결/호가 연계, 전략 실험 관리, 미국주식 확장 같은 방향으로 넘어갈 수 있어야 한다.

---

## 2. 이번 티켓에서 반드시 끝내야 하는 것

### 2.1 Training dataset assembly
다음이 가능한 상태를 만든다.

- `fact_feature_snapshot` 과 `fact_forward_return_label` 을 이용해 **horizon 별 supervised dataset** 을 만들 수 있어야 한다.
- 기본 타깃은 **시장 대비 초과수익률(`excess_return`)** 이다.
  - D+1: `next open -> D+1 close` 기준 초과수익률
  - D+5: `next open -> D+5 close` 기준 초과수익률
- 학습 행은 최소한 `(as_of_date, symbol, horizon)` 으로 식별 가능해야 한다.
- 학습 데이터셋은 아래 전처리 규칙을 가져야 한다.
  - label unavailable row 제외
  - 명시적 missing value 처리
  - feature leakage 금지
  - 학습 가능한 universe 필터 적용
- 학습 데이터셋의 생성 결과를 재현할 수 있도록 **dataset manifest** 가 남아야 한다.

### 2.2 정식 ML alpha model v1
다음이 가능한 상태를 만든다.

- `D+1`, `D+5` 를 분리한 **horizon-specific model family** 를 학습할 수 있어야 한다.
- 기본 구현은 **scikit-learn 기본 의존성만으로 동작**해야 한다.
- 최소한 아래 세 종류의 base model 을 사용한다.
  - `ElasticNetCV` 또는 동등한 선형/희소 선형 회귀
  - `HistGradientBoostingRegressor` 또는 동등한 트리 기반 boosting 회귀
  - `RandomForestRegressor` 또는 `ExtraTreesRegressor` 중 하나의 bagged tree 회귀
- base model 들을 동일한 타깃 스케일에서 학습하고, 각 모델의 예측값을 결합해 **ensemble alpha** 를 만든다.
- 가능하면 `MLPRegressor` 또는 optional model 을 추가할 수 있지만, **필수는 아니며 기본 경로는 sklearn만으로 재현 가능**해야 한다.
- LightGBM/XGBoost/SHAP 같은 외부 의존성은 **필수가 아니고 optional** 이어야 하며, 기본 구현이 그것들 없이는 돌아가지 않게 만들면 안 된다.

### 2.3 Walk-forward / expanding training discipline
다음이 가능한 상태를 만든다.

- 학습은 **time-aware** 해야 한다.
- 최소한 아래 원칙을 지킨다.
  - training cutoff 이전 데이터만 사용
  - inference 대상 `as_of_date` 이후 데이터 절대 사용 금지
  - scaler / encoder / imputer 는 train partition 기준 fit
  - validation / calibration 은 train 내부의 최근 구간 또는 OOF 예측 기준으로 수행
- 기본 전략은 **expanding window** 로 한다.
- 최소 학습 길이를 설정한다.
  - 예시: `min_train_days >= 120`
  - 데이터가 부족하면 모델 미생성 또는 fallback 처리
- validation 구간은 최근 고정 길이 또는 마지막 N 거래일로 둔다.
  - 예시: `validation_days = 20`
- 이 규칙은 코드와 README에서 명시되어야 한다.

### 2.4 OOF prediction + validation metric layer
다음이 가능한 상태를 만든다.

- 각 base model 에 대해 **OOF(out-of-fold) 또는 time-split validation prediction** 을 남길 수 있어야 한다.
- OOF 또는 validation prediction 을 이용해 아래를 계산한다.
  - point forecast error
  - rank correlation / Spearman correlation
  - sign hit rate
  - top bucket vs bottom bucket spread
  - pseudo-portfolio metric (top 10 / top 20 mean excess return)
- 이 metric 들은 **모델 가중치 산정, interval calibration, model health monitoring** 에 사용된다.
- 결과는 run metadata 와 함께 저장된다.

### 2.5 Ensemble alpha construction
다음이 가능한 상태를 만든다.

- base model 예측을 결합해 최종 `predicted_excess_return` 을 만든다.
- 결합 방식은 최소한 아래 중 하나를 명시적으로 구현한다.
  - validation metric 기반 fixed weight ensemble
  - 최근 window 성과 기반 normalized weight ensemble
  - equal weight ensemble + metric monitoring
- 어떤 방식이든 아래가 가능해야 한다.
  - model weight 저장
  - horizon 별 weight 분리
  - run 단위 재현 가능
- 최종 ensemble prediction 은 `prediction_source = ml_alpha_v1` 또는 동등한 명시적 source 값을 가져야 한다.

### 2.6 Uncertainty v1
다음이 가능한 상태를 만든다.

- rule-based proxy 가 아니라 **실제 모델 결과와 validation residual** 을 사용한 uncertainty 계층이 있어야 한다.
- 최소한 아래 두 가지를 결합한 uncertainty framework 를 구현한다.
  1. **ensemble dispersion**: base model 예측값의 표준편차/중앙절대편차
  2. **residual calibration**: validation 또는 OOF residual 의 분포를 이용한 interval half-width
- 결과로 아래 값을 제공한다.
  - `predicted_excess_return`
  - `prediction_interval_lower`
  - `prediction_interval_median`
  - `prediction_interval_upper`
  - `uncertainty_raw`
  - `uncertainty_score`
- 불확실성 구간은 최소한 아래 성질을 가져야 한다.
  - horizon 별 분리
  - 최근 validation history 기준 보정
  - 점추정과 별도로 저장
- interval 이 정교한 베이지안 불확실성인 척 설명하면 안 된다.
- README 에 **model-calibrated uncertainty v1** 임을 분명히 적는다.

### 2.7 Disagreement v1
다음이 가능한 상태를 만든다.

- 모델별 예측이 얼마나 갈리는지 측정할 수 있어야 한다.
- 최소한 아래 값을 계산한다.
  - model member prediction standard deviation
  - sign agreement ratio
  - max-min spread
  - disagreement percentile / score
- disagreement 는 uncertainty 와 분리 저장되어야 한다.
- 둘은 관련이 있지만 같은 개념이 아니다.
  - uncertainty: “이 예측 자체가 얼마나 불안정한가”
  - disagreement: “모델들이 서로 얼마나 다른 그림을 보고 있는가”
- selection engine v2 에서는 disagreement 가 penalty 로 반영되어야 한다.

### 2.8 Selection Engine v2
다음이 가능한 상태를 만든다.

- TICKET-004의 selection engine v1 을 깨지 않고, 별도의 **selection engine v2** 를 구현한다.
- v2 의 핵심은 **실제 ML alpha 중심**이다.
- 권장 기본식은 아래와 같은 구조여야 한다.

```text
selection_value_v2
= predicted_excess_return
- lambda_u * uncertainty_penalty
- lambda_d * disagreement_penalty
- lambda_i * implementation_penalty
+ lambda_f * flow_adjustment
+ lambda_r * regime_adjustment
```

- 또는 이와 동등한 구조로 **lower-confidence-bound(LCB)** 중심 설계도 허용한다.
- 핵심은 아래다.
  - rule-based score 를 alpha 대신 쓰면 안 된다.
  - explanatory score 는 UI/설명용 보조 정보로 남기고, core selection input 은 `predicted_excess_return` 이어야 한다.
  - `flow`, `regime`, `implementation` 은 alpha 위의 adjustment/policy layer 로 작동해야 한다.
- 결과로 최소한 아래를 생성한다.
  - `selection_value_v2`
  - `selection_grade_v2`
  - `grade_detail_v2`
  - `selection_engine_version = v2`
  - `top_reason_tags_json`
  - `selection_component_summary_json`

### 2.9 Fallback / degradation policy
다음이 가능한 상태를 만든다.

- 데이터 부족, 학습 실패, 특정 horizon model unavailable 등 상황에서 시스템이 조용히 깨지면 안 된다.
- 최소한 아래 정책을 가진다.
  - `ml_alpha_v1 available` 인 경우 v2 사용
  - `ml_alpha_v1 unavailable` 이면 명시적으로 fallback
  - fallback 시 `selection_engine_v1` 또는 explanatory ranking 을 사용할 수 있지만, source/version 을 반드시 기록
- UI / Ops / README 에 fallback 여부가 드러나야 한다.
- fallback row 를 숨기지 않는다.

### 2.10 Model registry / artifact / metadata
다음이 가능한 상태를 만든다.

- 모델 학습 결과를 나중에 재현할 수 있어야 한다.
- 최소한 아래 정보가 남아야 한다.
  - model run id
  - horizon
  - train start / train end
  - validation window
  - feature version
  - label version
  - model family
  - hyperparameter snapshot
  - dataset row count
  - used feature count
  - training metrics
  - created artifact path
- artifact 는 로컬 파일 기반으로 저장해도 되지만, 경로/버전/생성시각이 registry 에 남아야 한다.
- artifact serialization 은 pickle/joblib 로 충분하지만, **버전/호환성 주의사항**을 README 에 적는다.

### 2.11 Model diagnostics / explainability snapshot
다음이 가능한 상태를 만든다.

- 완전한 로컬 설명 가능성(local explainability)까지는 아니어도, 최소한 **전역 진단(global diagnostics)** 을 남겨야 한다.
- 최소한 아래를 구현한다.
  - linear model coefficient summary
  - tree model permutation importance 또는 impurity-based importance summary
  - ensemble member weight summary
  - feature coverage / missingness summary
- 이를 통해 “현재 모델이 어떤 피처군을 강하게 보고 있는지”를 확인할 수 있어야 한다.
- UI 의 Ops / Model Diagnostics 영역에서 일부를 확인할 수 있어야 한다.

### 2.12 UI / Ops / Evaluation 확장
다음이 가능해야 한다.

- `Leaderboard` 에서 v1 / v2 결과를 선택적으로 비교할 수 있어야 한다.
- `Stock Workbench` 에서 종목별 아래 정보를 볼 수 있어야 한다.
  - predicted excess return
  - lower / median / upper interval
  - uncertainty score
  - disagreement score
  - implementation penalty
  - flow / regime adjustment
  - explanatory reason tags
- `Evaluation` 페이지에서 최소한 아래 비교가 가능해야 한다.
  - explanatory ranking v0 vs selection engine v1 vs selection engine v2
  - horizon 별 성과
  - top 10 / top 20 / grade bucket 비교
  - calibration / band coverage 변화
- `Ops` 에서 최소한 아래를 볼 수 있어야 한다.
  - latest model train run status
  - artifact path
  - latest inference run status
  - fallback usage count
  - training/validation metric summary

### 2.13 스크립트 / 엔트리포인트
다음 스크립트가 동작해야 한다.

- `scripts/build_model_training_dataset.py`
- `scripts/train_alpha_model_v1.py`
- `scripts/backfill_alpha_oof_predictions.py`
- `scripts/materialize_alpha_predictions_v1.py`
- `scripts/materialize_selection_engine_v2.py`
- `scripts/validate_alpha_model_v1.py`
- `scripts/compare_selection_engines.py`
- `scripts/render_model_diagnostic_report.py`

참고:
- `materialize_selection_engine_v1.py` 는 깨지면 안 된다.
- `fact_prediction`, `fact_ranking`, `fact_selection_outcome`, `fact_evaluation_summary` 를 가능한 한 재사용하되, 필요한 경우 명시적 확장 컬럼 또는 보조 테이블을 추가한다.
- UI 로딩 시 학습이 돌면 안 된다. 학습/추론은 스크립트 및 배치 워커에서만 수행한다.

---

## 3. 이번 티켓의 범위와 비범위

### 3.1 이번 티켓의 범위
- 학습용 supervised dataset assembly
- horizon 별 ML alpha model v1 학습
- OOF / validation prediction 생성
- ensemble alpha construction
- model-calibrated uncertainty v1
- disagreement v1
- selection engine v2
- model registry / artifact / metric 저장
- v1/v2 비교 리포트 및 UI 확장
- README / 실행 가이드 갱신
- 테스트 작성

### 3.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- transformer / attention / cross-asset sequence model
- 고빈도 장중 체결/호가 피처를 본격적으로 학습하는 모델
- GPU 전제 딥러닝 파이프라인
- 대규모 hyperparameter search / Optuna 같은 실험 플랫폼
- 자동 주문/자동매매
- 미국주식 확장
- 완전한 온라인 학습
- LLM 기반 뉴스 생성형 reasoning 을 모델 학습 파이프라인에 강제 포함
- SHAP 의존성을 필수로 만드는 고비용 explainability 레이어

즉, 이번 티켓은 **운영 가능한 tabular ML alpha v1 + uncertainty/disagreement + selection v2** 까지다.

---

## 4. Codex가 작업 시작 전에 반드시 확인할 것

Codex는 작업 시작 전에 아래 순서를 따른다.

1. 루트 경로 `D:\MyApps\StockMaster` 를 기준으로 현재 저장소 상태를 확인한다.
2. 아래 문서를 먼저 읽는다.
   - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
   - `TICKET_000_Foundation_and_First_Work_Package.md`
   - `TICKET_001_Universe_Calendar_Provider_Activation.md`
   - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
   - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
   - `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
   - `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
   - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
   - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
   - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
   - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
   - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
   - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
3. TICKET-005 가 불완전하다면, 이번 티켓에 직접 필요한 blocking issue 만 보완한다.
4. 기존 label 정의(next open -> future close), evaluation schema, selection engine v1 계약을 불필요하게 뒤집지 않는다.
5. 새로운 파일은 기존 `app/`, `config/`, `scripts/`, `tests/`, `docs/` 구조 안에 추가한다.

---

## 5. 이번 티켓의 설계 원칙

### 5.1 설명용 점수와 ML alpha의 분리 유지
이번 티켓에서 가장 중요한 원칙이다.

- explanatory score 는 계속 **사용자 설명용 레이어** 다.
- selection engine v2 의 core input 은 **ML alpha point prediction** 이어야 한다.
- explanatory score 를 v2 의 사후 reason tag 또는 auxiliary monitoring 으로 쓰는 것은 허용한다.
- 하지만 explanatory score 자체를 alpha 대체재처럼 core formula 에 넣으면 안 된다.

### 5.2 기본 경로는 “sklearn-only, reproducible, cross-platform”
이 프로젝트는 개인용이며 Oracle Cloud Arm / 로컬 Windows / Docker 환경을 함께 고려한다.

따라서 기본 모델링 경로는 아래 원칙을 따른다.

- scikit-learn 만으로 동작해야 한다.
- 외부 C++ 빌드 의존성 없거나 최소화
- CPU 친화적
- 학습 시간 과도하게 길지 않음
- Docker 재현 용이

즉, 처음부터 LightGBM 필수화, GPU 전제, 복잡한 딥러닝 스택을 강제하지 않는다.

### 5.3 time-aware evaluation 절대 준수
- 랜덤 셔플 기반 CV 를 기본값으로 쓰면 안 된다.
- 날짜 순서가 있는 validation / OOF 원칙을 사용한다.
- model weighting, interval calibration, diagnostic 도 모두 시계열 순서를 존중해야 한다.

### 5.4 uncertainty 와 disagreement 는 분리
이 둘은 모두 penalty 로 쓰일 수 있지만 의미가 다르다.

- uncertainty: 이 예측이 스스로 얼마나 넓은 오차 가능성을 가지는가
- disagreement: 서로 다른 모델들이 같은 종목을 얼마나 다르게 보는가

둘을 같은 score 로 합쳐서 저장하지 말고, 원시값과 정규화 점수를 분리 보관한다.

### 5.5 selection engine v2 는 “alpha-first, policy-adjusted”
selection engine v2 는 rule-based selection v1 과 달라야 한다.

- `predicted_excess_return` 가 중심
- `uncertainty`, `disagreement`, `implementation` 은 감점 정책
- `flow`, `regime` 는 조정 항목
- 결과는 기대값이 아니라 **정책까지 반영한 selection value**

### 5.6 artifact / dataset manifest 는 필수
ML 티켓부터는 run manifest 만으로는 부족하다.

최소한 아래가 따로 남아야 한다.
- dataset manifest
- model artifact metadata
- training metric summary
- inference snapshot metadata
- fallback usage log

### 5.7 학습/추론/평가를 분리
- 학습 스크립트
- 추론(materialization) 스크립트
- 평가 스크립트
- UI 조회

이 네 가지를 섞지 않는다.

---

## 6. 모델링 설계 상세

### 6.1 학습 타깃
기본 타깃은 `fact_forward_return_label` 의 **시장 대비 초과수익률** 이다.

권장 필드 예시:
- `label_horizon`
- `raw_forward_return`
- `benchmark_forward_return`
- `excess_forward_return`
- `label_available`

학습 기본 타깃:
- D+1: `target = excess_forward_return` where `label_horizon = 1`
- D+5: `target = excess_forward_return` where `label_horizon = 5`

선택적 보조 타깃은 허용하지만, 이번 티켓의 정식 출력은 excess return 기준이어야 한다.

### 6.2 학습 가능 universe 규칙
기본적으로 아래 조건을 만족하는 행만 학습에 사용한다.

- `vw_universe_active_common_stock` 에 속함
- 필수 피처 coverage 충족
- label available
- 중대한 데이터 이상 플래그가 없는 경우
- 극단적 결측/정지 종목 제외

세부 exclude 규칙은 코드와 README 에 명시한다.

### 6.3 피처 입력 규칙
입력 피처는 TICKET-003/004 에서 생성된 snapshot 을 사용한다.

최소한 아래 그룹을 지원한다.
- price / momentum
- volatility / gap / drawdown
- volume / liquidity / turnover
- flow
- fundamentals / quality / value
- news / catalyst / news burst
- regime-linked contextual features
- data quality / missingness flags

주의:
- 미래 데이터로 계산된 피처 금지
- horizon 과 직접 충돌하는 미래 기반 요약 금지
- evaluation 결과를 feature 로 넣지 말 것

### 6.4 전처리 규칙
최소한 아래 원칙을 구현한다.

- feature list 고정 및 manifest 저장
- 숫자형 feature 위주
- 필요 시 clip / winsorization 허용
- 결측치는 deterministic rule 로 impute
- `is_imputed` 계열 flag 유지 권장
- linear model 계열은 train-based scaling 적용
- tree 계열은 scaling optional

### 6.5 base model family 권장안
필수 기본 구현:

1. Linear sparse baseline
- `ElasticNetCV`
- 목적: 안정적 baseline, 계수 해석, 과적합 방어

2. Tree boosting baseline
- `HistGradientBoostingRegressor`
- 목적: 비선형/상호작용 반영

3. Bagged tree baseline
- `RandomForestRegressor` 또는 `ExtraTreesRegressor`
- 목적: 예측 다양성 확보, disagreement 계산에 기여

옵션:
- `MLPRegressor` 추가 가능
- 하지만 학습 안정성/속도/재현성을 해치면 필수 경로에 넣지 않는다.

### 6.6 validation / OOF 전략
권장 최소 구현은 아래 둘 중 하나다.

1. Rolling time split validation
- train end 이전의 여러 cut 에서 반복
- 각 split 마다 validation chunk 예측 저장

2. Purged-style simple walk-forward OOF
- 날짜 축을 따라 미래 chunk 를 validation 으로 사용
- 과거 구간만으로 fit

어느 방식을 쓰든 아래가 중요하다.
- validation row 는 “그 row 시점 이전 데이터로 학습된 모델의 예측값” 이어야 한다.
- OOF/validation 결과를 interval calibration 과 model weighting 의 기준으로 쓴다.

### 6.7 ensemble weighting
가중치는 아래 중 하나로 구현한다.

- equal weight
- validation metric 기반 weight
- rank correlation / top bucket spread 기반 normalized weight

권장 규칙:
- horizon 별로 독립 weight
- 음수 weight 금지
- weight 합은 1
- 너무 작은 weight 는 0 으로 클리핑 가능

### 6.8 uncertainty v1 구현 권장안
권장 기본 구현:

1. 각 base model 의 예측값을 수집
2. ensemble point prediction 계산
3. validation / OOF residual history 를 이용해 **prediction bin 별 residual quantile** 생성
4. 현재 예측값이 속한 bin 에서 residual q25/q50/q75 또는 abs residual q80/q90 를 가져옴
5. dispersion 과 residual scale 을 결합해 interval width 를 산출

예시:

```text
uncertainty_raw = a * member_prediction_std + b * calibrated_abs_residual_quantile
prediction_interval_lower = predicted_excess_return - interval_half_width
prediction_interval_upper = predicted_excess_return + interval_half_width
```

중요:
- 전형적인 conformal / Bayesian 엄밀성까지는 요구하지 않는다.
- 그러나 validation history 기반 보정은 반드시 있어야 한다.
- proxy band 와 actual model band 는 source/version 으로 구분되어야 한다.

### 6.9 disagreement v1 구현 권장안
최소 구현:
- `member_prediction_std`
- `member_prediction_range`
- `sign_agreement_ratio`
- `member_count`

정규화:
- cross-sectional percentile 또는 recent trailing percentile 로 `disagreement_score` 생성

권장 의미 해석:
- 낮음: 모델들이 대체로 같은 방향/크기를 본다.
- 높음: 특정 종목/장세에서 모델이 서로 상충된 신호를 낸다.

### 6.10 selection engine v2 등급 규칙
권장 기본 규칙:

- `A`:
  - `selection_value_v2` 상위 구간
  - `prediction_interval_lower > 0`
  - disagreement / implementation penalty 허용 범위 내
- `A-`:
  - 기대값은 높지만 uncertainty 또는 disagreement 중 하나가 다소 높음
- `B`:
  - 기대값은 양수이나 robust conviction 이 약함
- `C`:
  - 기대값이 낮거나 penalty 후 매력도 부족

등급 규칙은 완전히 하드코딩해도 되지만, README 와 코드 주석에서 설명 가능해야 한다.

### 6.11 fallback 규칙
권장 기본 정책:

- 학습 이력 부족 / artifact 없음 / inference 실패 / feature mismatch 발생 시
  - `prediction_source = fallback_selection_v1`
  - `selection_engine_version = v1_fallback`
  - fallback 사유 기록
- 동일 row 에서 v2 unavailable / v1 available 여부를 확인할 수 있어야 한다.

### 6.12 v1 대비 비교 관점
이번 티켓 구현 후 최소한 아래 비교가 가능해야 한다.

- v0 explanatory ranking vs v1 selection engine vs v2 selection engine
- D+1 / D+5 별 hit rate
- top 10 / top 20 realized excess return
- lower-band coverage
- false positive 감소 여부
- high disagreement names 의 성과 특성
- fallback rows 와 non-fallback rows 의 성과 차이

---

## 7. 저장 계약과 데이터 구조

### 7.1 기존 테이블을 최대한 재사용
가능하면 아래 기존 계약을 재사용한다.

- `fact_feature_snapshot`
- `fact_forward_return_label`
- `fact_market_regime_snapshot`
- `fact_prediction`
- `fact_ranking`
- `fact_selection_outcome`
- `fact_evaluation_summary`
- `fact_calibration_diagnostic`

### 7.2 이번 티켓에서 새로 필요한 권장 테이블
최소 권장 계약은 아래와 같다.

#### 7.2.1 `fact_model_training_run`
권장 최소 컬럼:
- `model_run_id`
- `model_version`
- `horizon`
- `train_start_date`
- `train_end_date`
- `validation_start_date`
- `validation_end_date`
- `feature_version`
- `label_version`
- `dataset_manifest_path`
- `artifact_manifest_path`
- `status`
- `created_at`

#### 7.2.2 `fact_model_member_prediction`
권장 최소 컬럼:
- `run_id`
- `as_of_date`
- `symbol`
- `horizon`
- `model_version`
- `model_family`
- `prediction_value`
- `prediction_rank_pct`
- `created_at`

#### 7.2.3 `fact_model_metric_summary`
권장 최소 컬럼:
- `model_run_id`
- `horizon`
- `model_family`
- `metric_name`
- `metric_value`
- `metric_scope`
- `created_at`

#### 7.2.4 `fact_model_feature_importance`
권장 최소 컬럼:
- `model_run_id`
- `horizon`
- `model_family`
- `feature_name`
- `importance_value`
- `importance_rank`
- `importance_method`
- `created_at`

### 7.3 `fact_prediction` 확장 권장 컬럼
최소한 아래를 지원하도록 확장한다.

- `prediction_source`
- `model_version`
- `predicted_excess_return`
- `prediction_interval_lower`
- `prediction_interval_median`
- `prediction_interval_upper`
- `uncertainty_raw`
- `uncertainty_score`
- `disagreement_raw`
- `disagreement_score`
- `fallback_used`
- `fallback_reason`
- `prediction_metadata_json`

### 7.4 `fact_ranking` 확장 권장 컬럼
최소한 아래를 지원하도록 확장한다.

- `selection_engine_version`
- `selection_value`
- `selection_grade`
- `selection_grade_detail`
- `alpha_component`
- `flow_adjustment_component`
- `regime_adjustment_component`
- `uncertainty_penalty_component`
- `disagreement_penalty_component`
- `implementation_penalty_component`
- `selection_component_summary_json`

### 7.5 artifact 경로 권장안
예시:

```text
artifacts/models/alpha_v1/horizon=1/train_end_date=2026-03-06/
artifacts/models/alpha_v1/horizon=5/train_end_date=2026-03-06/
artifacts/diagnostics/alpha_v1/horizon=1/train_end_date=2026-03-06/
```

### 7.6 dataset manifest 경로 권장안
예시:

```text
data/curated/model_training/alpha_v1/horizon=1/train_end_date=2026-03-06/dataset_manifest.json
```

---

## 8. UI / 화면 요구사항

### 8.1 Leaderboard
최소한 아래가 보여야 한다.
- 엔진 버전 선택 (`v1`, `v2`, 필요 시 `explanatory`)
- predicted excess return
- lower / upper interval
- uncertainty score
- disagreement score
- fallback 여부
- A / A- / B / C 등급

### 8.2 Stock Workbench
최소한 아래가 보여야 한다.
- 종목별 최신 v2 예측값
- v1 대비 변화
- member model prediction strip 또는 요약
- uncertainty / disagreement 설명
- top reason tags
- risk flags
- 최근 outcome / postmortem 링크 또는 요약

### 8.3 Evaluation
최소한 아래가 보여야 한다.
- v0 / v1 / v2 비교
- horizon 별 성과
- grade bucket 별 성과
- fallback cohort 성과
- disagreement high vs low cohort 성과
- uncertainty bucket 별 realized outcome

### 8.4 Ops / Model Diagnostics
최소한 아래가 보여야 한다.
- latest train run
- latest inference run
- artifact path
- dataset row count
- horizon 별 model metric summary
- top feature importance
- fallback count
- validation health indicator

---

## 9. README / 문서화 요구사항

README 또는 동등 문서에는 최소한 아래를 적어야 한다.

- ML alpha model v1 의 목적과 한계
- 학습 타깃 정의 (excess return)
- expanding window / validation 규칙
- base model family 구성
- ensemble weighting 방식
- uncertainty v1 정의
- disagreement v1 정의
- selection engine v2 정의
- fallback 정책
- artifact / model registry 구조
- 재학습 / 추론 / 평가 실행 명령
- 현재 known limitations

---

## 10. 테스트 요구사항

최소한 아래 테스트가 있어야 한다.

### 10.1 Dataset assembly test
- feature / label join 시 leakage 없는지
- unavailable label row 제외되는지
- dataset manifest 가 생성되는지

### 10.2 Training run smoke test
- 소규모 샘플로 horizon 별 학습이 끝나는지
- artifact 저장 및 metadata 기록되는지

### 10.3 Prediction materialization test
- member prediction / ensemble prediction 이 저장되는지
- uncertainty / disagreement 값이 생성되는지
- fallback 조건이 명확히 동작하는지

### 10.4 Selection engine v2 test
- penalty/adjustment component 가 계산되는지
- selection grade 가 부여되는지
- v1 데이터와 충돌 없이 공존하는지

### 10.5 Evaluation compatibility test
- TICKET-005 평가 파이프라인이 v2 결과를 읽고 깨지지 않는지
- v1 / v2 비교 집계가 가능한지

### 10.6 UI smoke test
- Leaderboard / Stock Workbench / Evaluation / Ops 가 최신 필드를 읽을 수 있는지

---

## 11. 완료 기준 (Definition of Done)

아래가 모두 충족되면 이번 티켓을 완료로 본다.

1. `python scripts/build_model_training_dataset.py --train-end-date 2026-03-06 --horizons 1 5 --min-train-days 120`
2. `python scripts/train_alpha_model_v1.py --train-end-date 2026-03-06 --horizons 1 5 --min-train-days 120 --validation-days 20`
3. `python scripts/backfill_alpha_oof_predictions.py --start-train-end-date 2026-02-14 --end-train-end-date 2026-03-06 --horizons 1 5 --limit-models 3`
4. `python scripts/materialize_alpha_predictions_v1.py --as-of-date 2026-03-06 --horizons 1 5`
5. `python scripts/materialize_selection_engine_v2.py --as-of-date 2026-03-06 --horizons 1 5`
6. `python scripts/validate_alpha_model_v1.py --as-of-date 2026-03-06 --horizons 1 5`
7. `python scripts/compare_selection_engines.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5`
8. `python scripts/render_model_diagnostic_report.py --train-end-date 2026-03-06 --horizons 1 5 --dry-run`
9. `streamlit run app/ui/Home.py`
10. UI 에서 v2 예측값 / uncertainty / disagreement / fallback 정보 확인 가능
11. Evaluation 에서 v0 / v1 / v2 비교 가능
12. README 갱신 완료
13. 테스트 통과

---

## 12. 하지 말아야 할 것

이번 티켓에서 아래는 금지한다.

- UI 로딩 시 모델 학습 자동 실행
- 랜덤 셔플 기반 CV 를 기본값으로 사용
- explanatory score 를 alpha 대체재처럼 selection v2 core 에 넣기
- uncertainty 와 disagreement 를 구분 없이 하나의 숫자로만 저장
- fallback 발생 시 그 사실을 숨기기
- 외부 무거운 의존성을 필수화하여 기본 설치를 깨기
- 미래 정보를 사용한 calibration / model weighting
- 평가 시점에 과거 prediction snapshot 재계산 후 덮어쓰기
- 뉴스 본문 전문 저장/전송
- auto order / trading 기능 추가

---

## 13. 작업 완료 후 Codex가 남겨야 할 요약

작업이 끝나면 최소한 아래를 짧게 정리한다.

- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- 학습 데이터셋 생성 순서
- 모델 학습/OOF/추론/materialization 순서
- uncertainty / disagreement 계산 방식 요약
- selection engine v2 수식 요약
- fallback 정책 요약
- UI 에서 확인할 위치
- 아직 남은 TODO
- TICKET-007 진입 전 주의사항

