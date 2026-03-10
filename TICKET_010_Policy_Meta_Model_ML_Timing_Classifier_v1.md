# TICKET-010 — 장중 Policy Meta-Model / ML Timing Classifier v1

- 문서 목적: TICKET-009 이후, Codex가 바로 이어서 구현할 **장중 policy overlay를 위한 ML timing meta-model v1 + 보수적 최종 action decision layer** 의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
  - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
  - `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
  - `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
  - `TICKET_006_ML_Alpha_Uncertainty_Disagreement_Selection_v2.md`
  - `TICKET_007_Intraday_Candidate_Assist_Engine.md`
  - `TICKET_008_Intraday_Postmortem_Regime_Aware_Strategy_Comparison.md`
  - `TICKET_009_Policy_Calibration_Regime_Tuning_Experiment_Ablation.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
  - `CODEX_NINTH_INSTRUCTION_StockMaster.md`
  - `CODEX_TENTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
  - TICKET-005 postmortem / evaluation / calibration diagnostic 완료
  - TICKET-006 ML alpha model v1 / uncertainty-disagreement / selection engine v2 완료
  - TICKET-007 intraday candidate assist engine v1 완료
  - TICKET-008 intraday postmortem / regime-aware comparison / strategy comparison 완료
  - TICKET-009 intraday policy calibration / tuning / ablation / freeze-rollback 프레임 완료
- 우선순위: 최상
- 기대 결과: **활성화된 intraday policy 위에 보수적으로 올라가는 ML timing overlay가 도입되어, 후보군 장중 판단을 `규칙-only` 에서 `정책+ML overlay` 구조로 진화시키고, 그 overlay 자체도 재현 가능한 학습/예측/승격/롤백 체계 아래 관리되는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“활성 intraday policy가 내놓은 adjusted action을 그대로 쓰는 단계”에서 “활성 policy를 기반으로 하되, 과거 matured outcome에서 학습한 ML timing overlay가 필요한 경우에만 보수적으로 action을 미세 조정하는 단계”** 로 넘어가는 작업이다.

핵심은 다음 세 가지다.

1. 장중 snapshot을 기반으로 **정책 유지 / 다운그레이드 / 제한적 업그레이드** 를 판단하는 ML meta-model v1 을 만든다.
2. meta-model이 직접 모든 것을 결정하게 하지 않고, **기존 active policy가 만든 action과 hard guard를 우선** 하도록 설계한다.
3. meta-model의 결과도 모델 버전, 학습 구간, calibration, coverage, fallback, 최종 적용 여부가 남도록 하여 **재현 가능한 운영 계층** 으로 만든다.

즉, 이번 티켓의 목표는 “장중 AI 매매”가 아니라 **selection v2 + active intraday policy + conservative ML overlay** 의 삼층 구조를 완성하는 것이다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 selection engine v2 와 active intraday policy 가 여전히 모체다
이번 티켓의 meta-model은 독립 엔진이 아니다.

반드시 아래 순서를 유지한다.

1. 전일 selection engine v2 가 후보군을 만든다.
2. active intraday policy 가 raw action / adjusted action / tuned action 을 만든다.
3. 그 다음에야 ML timing overlay 가 들어와 **최종 action을 보수적으로 조정** 할 수 있다.

즉 ML overlay 는:
- 후보군 선정을 대체하지 않는다.
- 장중 active policy 체계를 대체하지 않는다.
- `AVOID_TODAY` hard block 이나 `DATA_INSUFFICIENT` 를 임의로 뒤집지 않는다.
- v1 에서는 **bounded override** 만 허용한다.

### 2.2 meta-model 의 예측 대상은 “주가 방향”이 아니라 “정책 override 가치”다
이번 티켓의 핵심 차별점은 예측 타깃이다.

예측 대상은 아래와 같은 **정책 overlay decision** 이어야 한다.

- 현재 adjusted action 을 유지하는 것이 좋은가?
- 현재 `ENTER_NOW` 를 `WAIT_RECHECK` 로 낮추는 것이 좋은가?
- 현재 `ENTER_NOW` 또는 `WAIT_RECHECK` 를 `AVOID_TODAY` 로 낮추는 것이 좋은가?
- 현재 `WAIT_RECHECK` 를 `ENTER_NOW` 로 제한적으로 올리는 것이 좋은가?

즉, meta-model은 **절대적인 미래 방향성** 보다 **동일한 exit 정의 하에서 active policy action 대비 override가 유리한가** 를 예측해야 한다.

### 2.3 학습과 검증은 반드시 matured outcome + as-of feature discipline 을 지킨다
이번 티켓에서 누수 방지는 최우선이다.

반드시 아래를 지킨다.

- 학습용 라벨은 TICKET-008 / TICKET-009 에서 확정된 **same-exit matured outcome** 만 사용한다.
- 해당 checkpoint 시점 이후에만 알 수 있는 값은 feature로 쓰면 안 된다.
- 같은 날짜에 freeze 할 active meta-model 을 선택할 때, 그 날짜 이후 outcome 을 사용하면 안 된다.
- calibration 과 threshold tuning 도 모두 **matured-only** 이어야 한다.
- live scoring 시점에는 그 시점까지 materialized 된 intraday snapshot 만 사용한다.

### 2.4 v1 은 sklearn-only, low-latency, conservative overlay 여야 한다
이번 티켓은 연구가 아니라 운영 가능한 1차 버전이어야 한다.

허용되는 것:
- `LogisticRegression` / `SGDClassifier` 계열
- `HistGradientBoostingClassifier`
- `RandomForestClassifier` / `ExtraTreesClassifier`
- 간단한 soft-voting / weighted-voting ensemble
- probability calibration
- rule-bounded final decision layer

이번 티켓에서 하지 않는 것:
- deep learning
- transformer
- sequence model
- reinforcement learning
- online learning
- per-tick ultra-low-latency model
- end-to-end autonomous action generation

### 2.5 모델은 “직접 액션을 생성” 하기보다 “현재 정책을 덮어쓸지” 판단한다
v1 은 policy overlay 이다.

따라서 모델 구조는 **현재 adjusted action 패널(panel)** 기준으로 나누는 것이 맞다.

권장 패널은 아래 두 개다.

- `ENTER_PANEL`: 현재 adjusted action 이 `ENTER_NOW` 인 snapshot 들
- `WAIT_PANEL`: 현재 adjusted action 이 `WAIT_RECHECK` 인 snapshot 들

`AVOID_TODAY` 와 `DATA_INSUFFICIENT` 는 v1 에서 meta-model 의 upward override 대상이 아니다.  
이 두 상태는 기본적으로 `KEEP_POLICY` 로 간주한다.

### 2.6 meta-model 은 hard guard 를 절대 넘지 못한다
아래 경우에는 meta-model 이 어떤 확률을 내더라도 final action 을 바꾸면 안 된다.

- `DATA_INSUFFICIENT`
- liquidity / tradability hard failure
- known gap guard hard block
- candidate snapshot 자체가 불완전한 상태
- policy registry 상 `do_not_override=true` 로 표기된 scope
- active policy 와 active meta-model scope mismatch

즉 v1 은 **policy-aware bounded overlay** 여야 한다.

### 2.7 override 는 확률만으로 하지 말고 confidence margin / disagreement / uncertainty 를 함께 본다
v1 의 final action layer 는 아래를 함께 반영해야 한다.

- predicted class probability
- top1 - top2 confidence margin
- ensemble member disagreement
- model uncertainty proxy
- panel-specific minimum confidence threshold
- action-type specific guard threshold

즉 “확률 51%” 같은 애매한 상태에서는 override 하지 않고 **KEEP_POLICY** 로 돌아가야 한다.

### 2.8 fallback 은 failure 가 아니라 설계의 일부다
다음 상태는 정상 동작으로 간주한다.

- active meta-model 없음
- 해당 horizon/panel/scope 에 맞는 active meta-model 없음
- feature coverage 부족
- scoring runtime failure
- confidence 부족
- disagreement 과다
- uncertainty 과다

이 경우 final action 은 기존 adjusted action 으로 돌아가야 하며,  
반드시 `fallback_used`, `fallback_reason` 이 저장되어야 한다.

### 2.9 candidate-only 철학을 유지한다
이번 티켓에서도 전 종목 장중 전수저장은 금지한다.

- scoring 대상은 여전히 **selection v2 기반 candidate universe** 에 한정한다.
- 학습 데이터도 candidate snapshot / matured comparison 기반이어야 한다.
- 80GB 저장 예산을 지키기 위해 모델 입력 snapshot 은 필요한 요약 형태로만 저장한다.

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 Intraday meta-model training dataset assembly
다음이 가능한 상태를 만든다.

- TICKET-008 / TICKET-009 의 intraday strategy result, action history, regime context, checkpoint snapshot, selection v2 context 를 조인하여 **meta-model 학습용 dataset** 을 만든다.
- dataset 은 최소한 아래 단위를 지원해야 한다.
  - `session_date`
  - `symbol`
  - `horizon`
  - `checkpoint_ts`
  - `panel_name` (`ENTER_PANEL`, `WAIT_PANEL`)
  - `active_policy_action`
  - `realized same-exit outcome summary`
  - `derived meta label`
- 학습용 dataset manifest 가 남아야 한다.
- label derivation 에 사용된 threshold 들이 manifest 에 저장되어야 한다.

### 3.2 Meta label definition 고정
v1 의 라벨은 **정책 override class** 여야 한다.

권장 최소 라벨 체계는 아래와 같다.

#### 3.2.1 ENTER_PANEL class
현재 adjusted action 이 `ENTER_NOW` 일 때 허용 class:

- `KEEP_ENTER`
- `DOWNGRADE_WAIT`
- `DOWNGRADE_AVOID`

라벨 생성 원칙:
- `DOWNGRADE_AVOID`: `AVOID_TODAY` 가 `ENTER_NOW` 대비 손실회피 관점에서 의미 있게 우월할 때
- `DOWNGRADE_WAIT`: `WAIT_RECHECK` 가 `ENTER_NOW` 대비 same-exit outcome 에서 의미 있게 우월하고 avoid 우월성은 더 약할 때
- `KEEP_ENTER`: 나머지

#### 3.2.2 WAIT_PANEL class
현재 adjusted action 이 `WAIT_RECHECK` 일 때 허용 class:

- `KEEP_WAIT`
- `UPGRADE_ENTER`
- `DOWNGRADE_AVOID`

라벨 생성 원칙:
- `UPGRADE_ENTER`: 지금 바로 들어가는 것이 기다리는 것보다 의미 있게 우월할 때
- `DOWNGRADE_AVOID`: 기다리는 것보다 피하는 것이 의미 있게 우월할 때
- `KEEP_WAIT`: 나머지

#### 3.2.3 Threshold discipline
최소한 아래 threshold 는 config 또는 registry 로 관리해야 한다.

- `enter_vs_wait_delta_bps`
- `wait_vs_enter_delta_bps`
- `avoid_vs_enter_delta_bps`
- `avoid_vs_wait_delta_bps`
- `min_effective_trade_outcome_bps`
- `label_noise_buffer_bps`

이 threshold 들은 하드코딩하지 말고 manifest / config / registry 에 남긴다.

### 3.3 Feature spec v1 확정
다음 피처군을 최소 지원한다.

#### 3.3.1 장전/전일 static context
- selection v2 rank / grade / selection value
- predicted excess return / interval / uncertainty / disagreement
- flow score / explanatory score 요약
- 전일 기술지표 요약
- 유동성/거래대금/회전율 bucket
- quality / risk penalty bucket

#### 3.3.2 당일 checkpoint intraday bar feature
- open 대비 현재 수익률
- 직전 1~3 bar momentum
- intraday high-low range ratio
- current bar return
- cumulative volume participation
- VWAP 또는 VWAP proxy distance
- bar volatility proxy
- gap size bucket
- upper/lower limit proximity proxy

#### 3.3.3 시장 / 장세 context
- KOSPI / KOSDAQ intraday return
- 시장 breadth proxy
- 당일 market regime family / cluster
- risk-on / risk-off regime indicator
- sector / cohort relative strength proxy (가능한 범위)
- 당일 장 초반 충격성 변동성 proxy

#### 3.3.4 정책/규칙 context
- raw action
- adjusted action
- active policy template
- active policy scope
- policy parameter summary
- guard flag summary
- friction / gap / cohort / breadth guard state
- tuning profile / adjustment profile id

#### 3.3.5 체크포인트 메타
- checkpoint index
- minute from open
- first signal elapsed minutes
- intraday revisit count
- score availability flags
- missingness flags

반드시 할 것:
- feature contract 문서화
- categorical encoding / missing handling 명시
- feature version 관리

### 3.4 Model family v1 구현
최소한 아래 구조를 구현한다.

#### 3.4.1 Panel-specific model
- `ENTER_PANEL` 과 `WAIT_PANEL` 을 분리 학습한다.
- 각 panel 별 최소 2개 이상 model family 를 학습한다.
- 권장 family:
  - `LogisticRegression`
  - `HistGradientBoostingClassifier`
  - `RandomForestClassifier` 또는 `ExtraTreesClassifier`

#### 3.4.2 Ensemble overlay
- panel 별 member 모델들의 class probability 를 합성하는 **soft-voting 또는 weighted ensemble** 을 구현한다.
- ensemble weight 는 validation 기반으로 정하되, 과도하게 복잡하게 만들지 않는다.
- 필요 시 equal-weight fallback 을 둔다.

#### 3.4.3 Calibration
- class probability calibration 을 넣는다.
- 최소한 아래 중 하나를 지원한다.
  - sigmoid/Platt style
  - isotonic (표본 충분 시)
- calibration 도 walk-forward discipline 을 지켜야 한다.

### 3.5 Walk-forward / temporal validation discipline
다음이 가능해야 한다.

- `ENTER_PANEL`, `WAIT_PANEL` 별로 anchored 또는 rolling walk-forward 학습/검증을 수행한다.
- validation 결과가 남아야 한다.
- 동일 날짜 leakage 없이 historical split 이 잡혀야 한다.
- 표본 수가 부족한 panel/scope 에 대해서는 global fallback 을 사용한다.

최소 split 단위:
- `horizon=1`, `horizon=5`
- panel 별
- 가능하면 regime family / checkpoint band 별 진단

### 3.6 Meta decision layer v1 구현
이 티켓의 핵심은 학습 자체보다 **최종 action 적용 계층** 이다.

반드시 아래 구조를 구현한다.

#### 3.6.1 입력
- active policy 의 adjusted action
- active meta-model 의 class probability
- confidence margin
- disagreement / uncertainty proxy
- hard guard 상태
- panel-specific override threshold

#### 3.6.2 출력
다음 중 하나를 만들 수 있어야 한다.

- `KEEP_POLICY`
- `OVERRIDE_TO_WAIT`
- `OVERRIDE_TO_AVOID`
- `OVERRIDE_TO_ENTER`

#### 3.6.3 허용 규칙
- 현재 adjusted action 이 `ENTER_NOW` 인 경우:
  - `KEEP_POLICY`
  - `OVERRIDE_TO_WAIT`
  - `OVERRIDE_TO_AVOID`
- 현재 adjusted action 이 `WAIT_RECHECK` 인 경우:
  - `KEEP_POLICY`
  - `OVERRIDE_TO_ENTER`
  - `OVERRIDE_TO_AVOID`
- 현재 adjusted action 이 `AVOID_TODAY` 또는 `DATA_INSUFFICIENT` 인 경우:
  - v1 에서는 `KEEP_POLICY` 만 허용

#### 3.6.4 bounded overlay rule
v1 은 아래를 반드시 지킨다.

- hard guard block 이 있으면 override 금지
- active policy 가 `DEFENSIVE_STRONG` 또는 동등한 hard defensive 상태일 때 상향 override 더 엄격
- `OVERRIDE_TO_ENTER` 는 `WAIT_PANEL` 에서만, 높은 confidence / 낮은 disagreement / 낮은 uncertainty 일 때만 허용
- `OVERRIDE_TO_AVOID` 는 상대적으로 완화된 기준이 가능하나, coverage 와 과도한 skip 을 같이 본다
- 불확실성 높으면 KEEP_POLICY 로 돌아간다

### 3.7 Registry / artifact / 저장 계약
이번 티켓이 끝나면 아래 정보가 저장되어야 한다.

#### 3.7.1 `fact_model_training_run` 확장
가능하면 기존 generic registry 를 재사용하고 아래 컬럼을 추가/활용한다.

권장 최소 컬럼:
- `model_run_id`
- `model_domain` (`intraday_meta`)
- `model_version`
- `panel_name`
- `horizon`
- `scope_type`
- `scope_value`
- `train_start_date`
- `train_end_date`
- `validation_start_date`
- `validation_end_date`
- `feature_version`
- `label_version`
- `dataset_manifest_path`
- `artifact_manifest_path`
- `calibration_manifest_path`
- `status`
- `created_at`

#### 3.7.2 `fact_model_metric_summary` 확장
권장 최소 컬럼:
- `model_run_id`
- `model_domain`
- `panel_name`
- `horizon`
- `metric_name`
- `metric_scope`
- `metric_value`
- `created_at`

#### 3.7.3 `fact_intraday_meta_prediction`
권장 최소 컬럼:
- `run_id`
- `session_date`
- `symbol`
- `checkpoint_ts`
- `horizon`
- `panel_name`
- `active_policy_id`
- `active_policy_action`
- `model_run_id`
- `model_version`
- `predicted_class`
- `prob_keep`
- `prob_action_alt_1`
- `prob_action_alt_2`
- `top_probability`
- `confidence_margin`
- `uncertainty_raw`
- `uncertainty_score`
- `disagreement_raw`
- `disagreement_score`
- `fallback_used`
- `fallback_reason`
- `prediction_metadata_json`
- `created_at`

#### 3.7.4 `fact_intraday_meta_decision`
권장 최소 컬럼:
- `run_id`
- `session_date`
- `symbol`
- `checkpoint_ts`
- `horizon`
- `raw_action`
- `adjusted_action`
- `ml_predicted_class`
- `final_action`
- `decision_source` (`POLICY_ONLY`, `META_OVERLAY`, `META_FALLBACK`)
- `override_applied`
- `override_direction` (`NONE`, `DOWNGRADE`, `UPGRADE`)
- `hard_guard_blocked`
- `active_policy_id`
- `active_meta_model_id`
- `decision_metadata_json`
- `created_at`

#### 3.7.5 `fact_intraday_active_meta_model`
권장 최소 컬럼:
- `active_meta_model_id`
- `model_run_id`
- `model_version`
- `panel_name`
- `horizon`
- `scope_type`
- `scope_value`
- `effective_from_date`
- `effective_to_date`
- `activation_reason`
- `rollback_of_id`
- `status`
- `created_at`

#### 3.7.6 `fact_model_feature_importance` 또는 동등 진단
- global coefficient / importance summary
- panel 별 importance
- horizon 별 importance
- 생성 시각 / method / run id

### 3.8 Active meta-model freeze / rollback 체계
이번 티켓에서도 **auto-promotion 금지** 원칙을 유지한다.

다음이 가능해야 한다.

- 특정 panel/horizon/scope 에 대해 active meta-model 을 수동 freeze
- 현재 active meta-model rollback
- overlapping active scope 금지
- active policy 와 active meta-model scope mismatch 시 validation failure
- weak evidence 상태의 aggressive promotion 차단

### 3.9 Inference / batch scoring / live-safe materialization
다음이 가능해야 한다.

- 특정 날짜 / checkpoint snapshot에 대해 active meta-model inference 실행
- candidate-only 대상 배치 scoring
- scoring 결과를 prediction table 과 decision table 로 materialize
- runtime failure 시 fallback reason 저장
- panel 미지원 또는 model 미존재 시 fallback

### 3.10 UI / Research / Ops 반영
다음이 가능해야 한다.

#### Research Lab / Model Diagnostics 페이지
- panel 별 성능 비교
- class distribution
- probability calibration
- disagreement / uncertainty 진단
- feature importance
- regime family 별 성과

#### Intraday Console
- raw action
- adjusted action
- ML predicted class
- class probabilities
- confidence margin
- disagreement / uncertainty
- final action
- override 이유 / fallback 이유

#### Evaluation 페이지
- policy-only vs meta-overlay 비교
- downgrade / upgrade precision
- action confusion matrix
- same-exit lift vs baseline
- regime / checkpoint band 별 성과

#### Ops 페이지
- active meta-model registry
- last scoring run
- fallback rate
- scoring error count
- artifact / manifest 경로
- rollback history

### 3.11 Research report / optional Discord summary
다음이 가능해야 한다.

- intraday meta-model 연구 리포트 생성
- 최소한 아래를 포함
  - active meta-model 요약
  - panel/horizon 성과
  - calibration 요약
  - override coverage
  - action-type precision
  - fallback 요약
  - top diagnostic notes
- 선택적으로 Discord 요약 발송 가능
- 단, verbose raw table 을 전송하지 말고 요약만 보낸다

---

## 4. 모델 설계 가이드

### 4.1 v1 에서 추천하는 패널 분리 방식
권장 기본안:

- `ENTER_PANEL`: `ENTER_NOW` 에서 override 필요성 판단
- `WAIT_PANEL`: `WAIT_RECHECK` 에서 upgrade/downgrade 판단

이 구조의 장점:
- label space 가 panel 별로 자연스럽다
- class imbalance 관리가 쉬워진다
- decision admissibility 가 명확하다
- hard defensive policy 와 상향 override 를 분리 제어하기 쉽다

### 4.2 class imbalance 대응
override class 는 희소할 가능성이 높다.  
최소한 아래 중 일부를 지원해야 한다.

- class weight
- resampling 또는 conservative undersampling
- threshold moving
- panel 별 minimum support 기준
- rare class 자동 비활성 fallback

### 4.3 uncertainty / disagreement proxy
정교한 Bayesian uncertainty 까지는 요구하지 않는다.  
v1 에서는 아래 조합이면 충분하다.

- ensemble member probability dispersion
- top1 probability margin
- bootstrap 또는 member variance proxy
- calibration residual 요약

이를 `uncertainty_score`, `disagreement_score` 형태로 저장한다.

### 4.4 conservative overlay 추천 규칙
권장 기본 방침:

- `DOWNGRADE_AVOID`: 상대적으로 가장 중요한 손실 회피 override
- `DOWNGRADE_WAIT`: 과열/초반 과매수 방지용
- `UPGRADE_ENTER`: 가장 제한적으로 허용

즉 v1 은 **공격적인 수익 확대** 보다 **나쁜 진입 회피** 를 더 중요하게 본다.

### 4.5 per-panel active threshold
panel 별로 아래 threshold 를 분리 관리하는 것을 권장한다.

- minimum class probability
- minimum confidence margin
- maximum uncertainty score
- maximum disagreement score
- maximum fallback tolerance
- class-specific minimum expected benefit proxy

---

## 5. 스크립트 요구사항

### 5.1 Dataset / label assembly
- `scripts/build_intraday_meta_training_dataset.py`
- `scripts/validate_intraday_meta_dataset.py`

### 5.2 Training / calibration / walk-forward
- `scripts/train_intraday_meta_models.py`
- `scripts/run_intraday_meta_walkforward.py`
- `scripts/calibrate_intraday_meta_thresholds.py`
- `scripts/evaluate_intraday_meta_models.py`

### 5.3 Inference / decision materialization
- `scripts/materialize_intraday_meta_predictions.py`
- `scripts/materialize_intraday_final_actions.py`

### 5.4 Registry / freeze / rollback
- `scripts/freeze_intraday_active_meta_model.py`
- `scripts/rollback_intraday_active_meta_model.py`

### 5.5 Report / validation
- `scripts/render_intraday_meta_model_report.py`
- `scripts/publish_discord_intraday_meta_summary.py` (optional)
- `scripts/validate_intraday_meta_model_framework.py`

모든 스크립트는:
- dry-run 지원
- 날짜 / horizon / panel / scope 지정 가능
- manifest 출력
- 실패 시 명확한 에러 메시지
- README 예시 포함

---

## 6. 최소 CLI 예시

```bash
python -m scripts.build_intraday_meta_training_dataset --start-date 2025-01-01 --end-date 2026-03-31
python -m scripts.validate_intraday_meta_dataset --dataset-version intraday_meta_v1
python -m scripts.train_intraday_meta_models --horizons 1 5 --panels ENTER_PANEL WAIT_PANEL --model-version intraday_meta_v1
python -m scripts.run_intraday_meta_walkforward --model-version intraday_meta_v1
python -m scripts.calibrate_intraday_meta_thresholds --model-version intraday_meta_v1
python -m scripts.evaluate_intraday_meta_models --model-version intraday_meta_v1
python -m scripts.freeze_intraday_active_meta_model --model-version intraday_meta_v1 --panel ENTER_PANEL --horizon 1 --scope-type GLOBAL
python -m scripts.materialize_intraday_meta_predictions --session-date 2026-03-07 --checkpoint 09:15
python -m scripts.materialize_intraday_final_actions --session-date 2026-03-07 --checkpoint 09:15
python -m scripts.render_intraday_meta_model_report --model-version intraday_meta_v1
python -m scripts.validate_intraday_meta_model_framework
```

---

## 7. 기본 평가 척도 요구사항

### 7.1 분류 성능
최소한 아래를 본다.

- panel 별 macro F1
- class 별 precision / recall / support
- multiclass log loss
- Brier score 또는 calibration proxy
- confusion matrix

### 7.2 overlay 실전 효용
가장 중요한 것은 아래다.

- `policy-only` 대비 `meta-overlay` same-exit 평균 성과 차이
- hit rate 변화
- 손실 회피 효과 (`saved loss`)
- 잘못 피한 수익 기회 (`missed winner`)
- upgrade 성공률
- downgrade 성공률

### 7.3 coverage / action mix
- override rate
- class prediction distribution
- final action distribution
- panel coverage
- horizon coverage
- regime family 별 coverage

### 7.4 안정성
- 기간별 drift
- regime 별 편차
- checkpoint band 별 성과 일관성
- high uncertainty 구간 성과
- active/fallback 비율

### 7.5 latency / operability
- candidate batch scoring wall time
- average per-row scoring latency
- failure rate
- artifact reload success
- live-safe inference 여부

---

## 8. UI 요구사항 세부

### 8.1 Research Lab / Model Diagnostics
최소한 아래가 보여야 한다.

- model version / panel / horizon 선택
- train / validation 기간
- class support
- calibration chart 또는 calibration table
- confusion matrix
- feature importance
- regime family / checkpoint band breakdown
- active model 여부

### 8.2 Intraday Console
최소한 아래가 보여야 한다.

- selection v2 context
- active policy action
- meta predicted class
- class probabilities
- top1 / top2 margin
- uncertainty / disagreement
- final action
- override applied 여부
- fallback reason
- applied active meta-model id

### 8.3 Evaluation 페이지
최소한 아래가 보여야 한다.

- `policy-only` vs `policy+meta-overlay`
- panel 별 performance
- downgrade / upgrade precision
- saved loss / missed winner
- regime family breakdown
- checkpoint band breakdown

### 8.4 Ops 페이지
최소한 아래가 보여야 한다.

- active meta-model registry
- 최근 training run
- 최근 scoring run
- artifact path / dataset manifest path
- fallback rate
- scoring errors
- rollback history

---

## 9. 테스트 요구사항

최소한 아래 테스트를 작성한다.

- label derivation 테스트
- as-of join / no leakage 테스트
- panel assignment 테스트
- class admissibility 테스트
- hard guard override 금지 테스트
- fallback path 테스트
- active meta-model overlap 금지 테스트
- freeze / rollback 테스트
- walk-forward split integrity 테스트
- prediction materialization schema 테스트
- final action decision logic 테스트
- calibration manifest presence 테스트

---

## 10. 이번 티켓에서 하지 말아야 할 것

- 자동매매 / 주문 API 연동
- end-to-end deep learning
- transformer / sequence model
- reinforcement learning
- online learning / real-time self-update
- tick-level 고빈도 전수 저장
- `AVOID_TODAY` hard block 을 upward override 하는 로직
- `DATA_INSUFFICIENT` 를 강제로 진입으로 바꾸는 로직
- active policy 없는 상태에서 meta-model 단독 action 생성
- auto-promotion / auto-rollback
- 뉴스 본문 전문 저장/전송
- UI 접속 시 training / calibration 자동 실행
- existing raw/adjusted decision overwrite

---

## 11. 완료 기준 (Definition of Done)

아래가 모두 만족되어야 한다.

1. matured intraday snapshot 기반 meta training dataset 을 생성할 수 있다.
2. `ENTER_PANEL`, `WAIT_PANEL` 별 학습/평가가 가능하다.
3. panel 별 calibrated probability 를 생성할 수 있다.
4. active meta-model 을 freeze / rollback 할 수 있다.
5. live-safe candidate batch scoring 이 가능하다.
6. scoring 결과를 prediction / final decision 테이블에 저장한다.
7. final action layer 가 hard guard / fallback / admissibility 를 지킨다.
8. Evaluation 에서 `policy-only` 와 `policy+meta-overlay` 를 비교할 수 있다.
9. Research/Ops 화면에서 active meta-model 상태를 볼 수 있다.
10. README 와 실행 예시가 갱신된다.
11. 테스트가 추가되고 핵심 경로가 통과한다.

---

## 12. README 에 반드시 추가할 것

- intraday meta-model 의 역할과 한계
- active policy 와의 관계
- panel 개념
- label derivation 원칙
- freeze / rollback 방법
- fallback 발생 의미
- scoring 실행 예시
- 디버깅 포인트
- 저장 경로 / artifact 경로 / manifest 경로
- 왜 v1 에서는 upward override 를 제한적으로만 허용하는지

---

## 13. Codex 작업 후 반드시 남겨야 하는 요약

Codex 는 작업 완료 후 아래를 텍스트로 남겨야 한다.

- 추가/수정한 파일 목록
- 생성한 스크립트 목록
- 학습/평가/추론 파이프라인 흐름
- 추가한 저장 테이블/컬럼
- active meta-model freeze / rollback 방식
- final action decision 규칙 요약
- known limitation
- 다음 티켓에 넘길 리스크/메모

---

## 14. 다음 티켓으로의 연결 의도

이번 티켓이 끝나면 다음 단계는 자연스럽게 아래 중 하나로 연결된다.

- **TICKET-011: Unified Portfolio / Capital Allocation / Risk Budgeting Layer**
- 또는 **TICKET-011: Cross-layer Decision Service (selection v2 + intraday policy + meta overlay 통합 의사결정 서비스)**

즉, 이번 티켓은 “무슨 종목을 볼지”와 “언제 들어갈지” 사이에 있는 마지막 ML overlay 를 만드는 단계이고,  
다음 단계는 그것을 **포지션 규모 / 포트폴리오 제약 / 실사용 의사결정 체계** 로 연결하는 단계가 된다.
