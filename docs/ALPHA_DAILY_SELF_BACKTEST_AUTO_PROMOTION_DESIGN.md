# Alpha Daily Self-Backtest Auto-Promotion Design

## 목적

이 설계의 핵심은 `매일 더 자주 학습한다`가 아니라, 아래를 하나의 자동 운영 루프로 묶는 것이다.

1. 오늘 추천한 종목이 실제로 얼마나 올랐는지 매일 같은 규칙으로 평가한다.
2. 그 셀프백테스트 결과를 기준으로만 alpha 후보를 비교한다.
3. 통계 검정을 통과한 후보만 active model 로 자동 승급한다.

즉, 자동학습/자동승급의 진실 원천은 `in-sample validation` 이 아니라 `스케줄화된 frozen recommendation self-backtest` 다.

---

## 현재 상태

현재 구조는 이미 절반 이상 준비되어 있다.

- daily close 번들은 alpha 학습을 끈 채 운영된다.
  - [app/ops/bundles.py](d:/MyApps/StockMaster/app/ops/bundles.py) 에서 `run_training=False`
- alpha 추론은 별도 active registry 없이 `최신 성공 training run` 을 바로 사용한다.
  - [app/ml/registry.py](d:/MyApps/StockMaster/app/ml/registry.py)
  - [app/ml/inference.py](d:/MyApps/StockMaster/app/ml/inference.py)
- 반면 evaluation 계층은 이미 존재한다.
  - [app/evaluation/outcomes.py](d:/MyApps/StockMaster/app/evaluation/outcomes.py)
  - [app/evaluation/summary.py](d:/MyApps/StockMaster/app/evaluation/summary.py)
  - [app/evaluation/calibration_diagnostics.py](d:/MyApps/StockMaster/app/evaluation/calibration_diagnostics.py)
- intraday 는 candidate -> recommendation -> active freeze 패턴이 이미 있다.
  - [app/intraday/policy.py](d:/MyApps/StockMaster/app/intraday/policy.py)
  - [app/intraday/meta_training.py](d:/MyApps/StockMaster/app/intraday/meta_training.py)

현재 gap 은 다섯 가지다.

1. alpha 에는 `active model registry` 가 없다.
2. `fact_prediction` / `fact_selection_outcome` 에 어떤 `training_run_id` / `active model id` 가 실제 추천을 만들었는지 남지 않는다.
3. evaluation summary 는 이미 유용하지만, 모델 spec 별 셀프백테스트 surface 는 없다.
4. 따라서 지금 상태에서 auto-promotion 을 붙이면 `왜 바뀌었는지` 와 `실제로 더 좋아졌는지` 를 audit 할 수 없다.
5. 현재 [app/evaluation/summary.py](d:/MyApps/StockMaster/app/evaluation/summary.py) 의 segment 는 `all/top_decile/report_candidates` 중심이라, `top5/top10/top20` basket self-backtest 를 승급 기준으로 직접 쓰기에는 확장이 필요하다.

---

## 설계 원칙

### 1. 승급 기준은 반드시 실전형 셀프백테스트여야 한다

사용자 관점의 핵심 질문은 두 개다.

- 내일 오를 것이라고 요약한 종목들이 실제로 얼마나 올랐는가
- 예측 수치가 실제 realized excess return 과 얼마나 가까웠는가

따라서 promotion 의 1차 기준은 `top recommendation basket 의 realized excess return`, 2차 진단은 `point forecast loss / rank consistency / calibration` 이다.

### 2. 추천 당시 snapshot 만 평가에 사용한다

TICKET-005 의 원칙을 그대로 유지한다.

- evaluation 시점에 예측을 다시 계산하지 않는다
- selection 당시 prediction / band / ranking snapshot 을 freeze 한다
- matured outcome 과 나중에 join 한다

### 3. 후보 비교는 논문에 있는 검정으로만 한다

임의 threshold 와 hand-tuned heuristic 조합으로 promotion 하지 않는다.

### 4. alpha 도 intraday 와 같은 운영 패턴을 가져간다

- candidate spec 생성
- candidate 평가
- recommendation 생성
- active freeze / rollback

단, intraday 와 달리 alpha 는 `추천 basket self-backtest` 가 승급 기준의 중심이다.

---

## 채택할 방법론과 적용 방식

### A. Cross-sectional expected return 평가

기준 문헌:

- Yichao Zhou, "Cross-sectional Expected Returns: New Fama-MacBeth Regressions in the Era of Machine Learning" (NBER Working Paper 33665, 2024)
  - https://www.nber.org/papers/w33665

적용 포인트:

- cross-sectional alpha 는 단순 RMSE 하나로 평가하지 않는다
- top bucket 성과, rank-based 성과, forecast combination 을 함께 본다
- StockMaster 에서는 아래를 고정 metric panel 로 사용한다
  - `top10 mean realized excess return`
  - `top20 mean realized excess return`
  - `rank IC` 또는 `selection percentile vs realized excess return correlation`
  - `squared error` of expected excess return

이 중 product objective 와 가장 가까운 `top10 mean realized excess return` 을 primary loss 로 둔다.

### B. Recursive vs rolling 학습을 둘 다 유지하고 결합

기준 문헌:

- Todd E. Clark and Michael W. McCracken, "Improving Forecast Accuracy by Combining Recursive and Rolling Forecasts" (International Economic Review, 2010)
  - https://ideas.repec.org/a/ier/iecrev/v51y2010i2p363-395.html

적용 포인트:

- 구조 변화가 있으면 rolling 이 recursive 보다 나을 수 있다
- 구조 변화가 약하면 recursive 가 더 안정적일 수 있다
- 따라서 둘 중 하나를 고정 선택하지 않고, 둘 다 candidate set 에 넣고 combination forecast 도 함께 운영한다

StockMaster 적용 형태:

- `alpha_recursive_expanding`
- `alpha_rolling_120`
- `alpha_rolling_250`
- `alpha_recursive_rolling_combo`

위 네 spec 을 small candidate set 으로 유지한다. 여기서 `120/250` 은 arbitrary tuning 값이 아니라 운영상 가장 흔한 중기/장기 거래일 창으로 고정하고, window choice uncertainty 는 아래 Rossi-Inoue 방식으로 처리한다.

### C. 단일 rolling window 를 정답처럼 고정하지 않는다

기준 문헌:

- Atsushi Inoue and Barbara Rossi, "Out-of-Sample Forecast Tests Robust to the Choice of Window Size" (Journal of Business and Economic Statistics, 2012)
  - https://ideas.repec.org/a/taf/jnlbes/v30y2012i3p432-453.html

적용 포인트:

- rolling window size 를 하나만 고정해 superiority 를 선언하면 왜곡될 수 있다
- 따라서 StockMaster 는 `rolling_120`, `rolling_250`, `recursive`, `combo` 를 동시에 candidate set 으로 유지하고
- superiority 판단은 특정 window 하나가 아니라 candidate set 전체 위에서 수행한다

즉, window size choice 자체를 heuristic 으로 숨기지 않고 candidate universe 로 명시한다.

### D. superior set 기반 자동승급

기준 문헌:

- Peter R. Hansen, Asger Lunde, James M. Nason, "The Model Confidence Set" (Econometrica, 2011)
  - https://ideas.repec.org/p/aah/create/2010-76.html

적용 포인트:

- 하나의 모델을 무조건 best 라고 찍는 대신, out-of-sample loss 기준 superior set 을 만든다
- incumbent 가 superior set 안에 남아 있으면 굳이 교체하지 않는다
- incumbent 가 superior set 에서 탈락하고 challenger 만 남으면 auto-promotion 할 수 있다

StockMaster 적용 규칙:

1. trailing matured selection dates 위에서 loss matrix 생성
2. horizon 별로 MCS 실행
3. incumbent active spec 이 superior set 에 남으면 유지
4. incumbent 가 탈락하고 challenger 가 superior set 에 남으면 승급 후보
5. 여러 challenger 가 남으면 `recursive_rolling_combo` 가 superior set 안에 있을 때만 combo 를 승급
6. combo 도 없고 survivor 가 복수면 자동승급하지 않고 recommendation 만 남긴다

이렇게 하면 tie-break 도 임의 점수 합산이 아니라 Clark-McCracken 조합 forecast 로 해결한다.

### E. 불안정 환경 진단은 설명 계층으로만 추가

기준 문헌:

- Raffaella Giacomini and Barbara Rossi, "Forecast Comparisons in Unstable Environments" (Journal of Applied Econometrics, 2010)
  - https://ideas.repec.org/a/jae/japmet/v25y2010i4p595-620.html

적용 포인트:

- local relative performance 는 regime 에 따라 달라질 수 있다
- 이 검정은 promotion trigger 자체보다 `왜 이번에 바뀌었는지` 를 설명하는 진단 계층으로 쓰는 편이 적합하다

따라서 Giacomini-Rossi 는 `promotion_report_json` 에 넣는 진단으로 사용한다.

---

## 핵심 설계: 셀프백테스트 중심 alpha 운영 루프

### 1. 개념 분리

- `model_spec_id`
  - 학습 방법론 단위
  - 예: `alpha_recursive_expanding_v1`
- `training_run_id`
  - 특정 거래일 종료 시점에 실제 학습된 artifact
- `active_alpha_model_id`
  - 현재 운영에 반영 중인 horizon 별 active pointer

승급은 `spec` 기준으로 판단하고, 실제 반영은 `오늘 생성된 training_run_id` 를 active registry 에 freeze 한다.

### 2. 오늘 생성하는 것은 두 종류다

1. production prediction
   - active model 만 사용
   - 기존 `fact_prediction` / `fact_ranking` 으로 노출
2. shadow candidate prediction
   - 모든 registered alpha spec 에 대해 생성
   - self-backtest 전용 저장소에 적재

이 구조가 있어야 미래에 matured outcome 이 생겼을 때 spec 별 실전 성과 attribution 이 가능하다.

### 3. 승급은 오늘 성과가 아니라 누적 matured history 로 한다

하루 학습 결과만 보고 바로 승급하지 않는다.

- 오늘 train 된 candidate run 은 오늘 shadow prediction 을 만든다
- 이후 D+1 / D+5 가 지나 matured outcome 이 생기면 self-backtest row 가 쌓인다
- promotion engine 은 최근 `60` matured selection dates 를 기준으로 각 spec 의 실전 성과를 비교한다

즉, 학습은 daily, 승급 기준은 trailing OOS self-backtest 다.

---

## 필요한 저장 계약

### 1. 신규 테이블

#### `dim_alpha_model_spec`

- `model_spec_id`
- `model_version`
- `estimation_scheme`
  - `recursive`
  - `rolling`
  - `combo`
- `rolling_window_days`
- `feature_version`
- `label_version`
- `selection_engine_version`
- `spec_payload_json`
- `active_candidate_flag`
- `created_at`

#### `fact_alpha_active_model`

intraday active meta-model 패턴을 그대로 따른다.

- `active_alpha_model_id`
- `horizon`
- `model_spec_id`
- `training_run_id`
- `model_version`
- `source_type`
- `promotion_type`
  - `AUTO_PROMOTION`
  - `MANUAL_FREEZE`
  - `ROLLBACK`
- `promotion_report_json`
- `effective_from_date`
- `effective_to_date`
- `active_flag`
- `rollback_of_active_alpha_model_id`
- `note`
- `created_at`
- `updated_at`

#### `fact_alpha_shadow_prediction`

- `selection_date`
- `symbol`
- `horizon`
- `model_spec_id`
- `training_run_id`
- `expected_excess_return`
- `lower_band`
- `median_band`
- `upper_band`
- `uncertainty_score`
- `disagreement_score`
- `created_at`

#### `fact_alpha_shadow_ranking`

- `selection_date`
- `symbol`
- `horizon`
- `model_spec_id`
- `training_run_id`
- `final_selection_value`
- `selection_percentile`
- `grade`
- `report_candidate_flag`
- `created_at`

#### `fact_alpha_shadow_selection_outcome`

production `fact_selection_outcome` 의 shadow candidate 버전이다.

- `selection_date`
- `evaluation_date`
- `symbol`
- `horizon`
- `model_spec_id`
- `training_run_id`
- `selection_percentile`
- `report_candidate_flag`
- `grade`
- `expected_excess_return_at_selection`
- `lower_band_at_selection`
- `upper_band_at_selection`
- `realized_excess_return`
- `prediction_error`
- `outcome_status`
- `created_at`
- `updated_at`

#### `fact_alpha_shadow_evaluation_summary`

- `summary_date`
- `window_type`
- `window_start`
- `window_end`
- `horizon`
- `model_spec_id`
- `segment_value`
  - `all`
  - `top5`
  - `top10`
  - `top20`
  - `report_candidates`
- `count_evaluated`
- `mean_realized_excess_return`
- `mean_point_loss`
- `rank_ic`
- `evaluation_run_id`
- `created_at`

#### `fact_alpha_promotion_test`

- `promotion_date`
- `horizon`
- `incumbent_model_spec_id`
- `challenger_model_spec_id`
- `loss_name`
- `window_start`
- `window_end`
- `sample_count`
- `mcs_member_flag`
- `incumbent_mcs_member_flag`
- `p_value`
- `decision`
  - `KEEP_ACTIVE`
  - `PROMOTE_CHALLENGER`
  - `NO_AUTO_PROMOTION`
- `detail_json`
- `created_at`

### 2. 기존 테이블 확장

#### `fact_model_training_run`

metadata 에만 숨기지 말고 top-level column 으로 남긴다.

- `model_spec_id`
- `estimation_scheme`
- `rolling_window_days`

#### `fact_prediction`

- `training_run_id`
- `model_spec_id`
- `active_alpha_model_id`

#### `fact_selection_outcome`

- `training_run_id_at_selection`
- `model_spec_id_at_selection`
- `active_alpha_model_id_at_selection`

이 세 컬럼이 있어야 production recommendation 의 실전 성과를 특정 active model lineage 로 귀속시킬 수 있다.

---

## 배치 순서

### 장후 평가 배치 (`16:20`)

기존 evaluation bundle 을 유지하되, 아래를 추가한다.

1. production recommendation matured outcome 적재
2. shadow candidate ranking matured outcome 적재
3. `fact_alpha_shadow_selection_outcome` 적재
4. spec 별 self-backtest summary 갱신

### daily close 배치 (`18:40`)

권장 순서:

1. close data / feature build
2. alpha candidate spec 학습
3. shadow candidate prediction + ranking materialize
4. trailing self-backtest 기반 promotion test 실행
5. auto-promotion 가능 시 `fact_alpha_active_model` freeze
6. active model 기준 production prediction + ranking materialize
7. report / portfolio / Discord

중요:

- promotion test 는 `오늘 train 결과` 를 보지만
- 판단 근거는 `오늘 이전까지 축적된 matured self-backtest` 다

즉, 오늘의 artifact 는 승급 대상이고, 승급 판단 데이터는 과거 frozen history 다.

---

## loss 정의

### primary loss

제품 목표와 직접 연결한다.

- `loss_top10 = - mean_realized_excess_return(top10 basket)`

이 값이 작을수록 좋다.

### secondary audit losses

- `loss_top20 = - mean_realized_excess_return(top20 basket)`
- `loss_point = mean((realized_excess_return - expected_excess_return)^2)`
- `loss_rank = - rank_ic`

정책:

- MCS 의 primary input 은 `loss_top10`
- `loss_top20`, `loss_point`, `loss_rank` 는 auto-promotion report 와 manual audit 에 항상 남긴다
- 향후 auto-promotion 을 더 보수적으로 만들고 싶으면 MCS intersection 대상으로 확장할 수 있지만, 초기 설계에서는 primary objective 를 명확히 유지한다

---

## 자동승급 규칙

### horizon 별 실행

alpha 는 horizon 단위로 실제 artifact 가 분리되어 있으므로 promotion 도 `D+1`, `D+5` 각각 독립적으로 수행한다.

### 규칙

1. trailing `60` matured selection dates 위에서 각 spec 의 `loss_top10` loss matrix 생성
2. horizon 별 MCS 실행
3. incumbent 가 MCS member 이면 `KEEP_ACTIVE`
4. incumbent 탈락 + challenger 1개만 생존이면 `PROMOTE_CHALLENGER`
5. incumbent 탈락 + challenger 복수 생존 + combo 생존이면 `PROMOTE_CHALLENGER(combo)`
6. 그 외는 `NO_AUTO_PROMOTION`

이 규칙은 임의 점수 합산 대신 `MCS superior set` 과 `recursive/rolling combination` 만으로 닫힌다.

---

## 왜 이 설계가 현재 코드에 바로 붙는다

### 이미 있는 것

- frozen outcome materialization
- rolling evaluation summary
- calibration diagnostic
- intraday candidate/active registry 패턴
- alpha OOF / validation metric / diagnostic report

### 새로 필요한 것

- alpha active registry
- shadow candidate prediction/ranking 저장
- shadow candidate outcome/summary 저장
- prediction/outcome 에 model lineage 기록
- MCS 기반 promotion test

즉, `generic backtester` 를 새로 만드는 것이 아니라 현재 TICKET-005 / TICKET-015 자산을 alpha promotion surface 로 재배선하는 작업이다.

---

## 구현 우선순위

### Phase 1

- `fact_alpha_active_model`
- `model_spec_id` 도입
- active registry 기반 alpha inference 로 전환
- auto-promotion 없이 manual freeze 만 지원

### Phase 2

- shadow candidate prediction / ranking 적재
- shadow self-backtest summary 적재
- 초기 backfill

### Phase 3

- MCS promotion test 구현
- daily close bundle 에 auto-promotion 삽입
- rollback CLI / UI 추가

### Phase 4

- Home / Evaluation / Ops 에 `active vs challenger self-backtest` 카드 추가
- Discord 장후 리포트에 `active kept / challenger promoted` 사유 추가

---

## 명시적 비채택

아래는 이번 설계의 중심에서 제외한다.

- training validation score 만으로 자동승급
- 하나의 rolling window 만 고정해 superiority 선언
- point metric 여러 개를 임의 가중합한 objective score
- active model 을 `latest successful run` 으로 암묵 선택하는 방식

---

## 결론

StockMaster 에 필요한 것은 `매일 자동학습` 자체보다 `매일 누적되는 실전형 셀프백테스트를 기반으로 한 alpha active registry` 다.

가장 작은 변경으로도 아래가 가능해진다.

- 내일 추천 basket 이 실제로 얼마나 맞았는지 model spec 별로 누적
- numeric prediction 이 얼마나 맞았는지 함께 진단
- recursive / rolling / combo 후보를 논문 기반 superior set 으로 비교
- active model 자동교체를 audit 가능한 형태로 운영

이 설계는 현재 evaluation/intraday 패턴을 재사용하면서도, promotion 로직만큼은 heuristic 이 아니라 논문 기반 검정 위에 올린다.
