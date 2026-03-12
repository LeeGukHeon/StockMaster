# TICKET-027

artifact / metadata 참조 강건화 리팩토링.

## 1. 배경

StockMaster는 운영 자동화가 강해지면서 artifact를 여러 경로에서 읽는다.

대표 예:

- alpha model artifact
- intraday meta model artifact
- diagnostic json
- report preview / payload path
- latest manifest artifact path

문제는 artifact 경로 해석이 레이어마다 달랐다는 점이다.

- UI helper만 legacy path fallback을 알고 있음
- ML inference는 `artifact_uri`를 생으로 열음
- intraday meta inference/calibration도 생으로 열음
- release validation / audit checks도 경로 존재 여부를 생으로 판단함

이 구조에서는 다음과 같은 일이 반복될 수 있다.

- host worker가 남긴 절대 경로를 app 컨테이너가 못 읽음
- cleanup 이후 legacy `app/data/artifacts` 경로가 남아 UI/validation이 깨짐
- 같은 artifact에 대해 UI는 읽고 validation은 실패하는 불일치 발생

즉 문제는 “파일이 없는가”가 아니라 “참조 해석 규칙이 일관되지 않다”는 점이다.

## 2. 목표

- artifact path 해석 규칙을 공통 모듈로 수렴한다.
- UI, inference, meta inference, calibration, audit, release validation이 같은 resolver를 쓴다.
- host path / container path / legacy path 차이로 인해 레이어별 성공/실패가 갈리지 않게 만든다.
- cleanup 이후에도 active training run / latest report artifact 참조가 깨지지 않게 한다.

## 3. 비목표

- artifact storage backend 자체 변경
- S3/object storage 도입
- metadata schema 대수술
- cleanup 정책 재설계 전체

## 4. 현재 문제 패턴

### 4.1 경로 불일치

- host worker는 `/opt/stockmaster/runtime/data/artifacts/...`
- container는 `/workspace/data/artifacts/...`
- legacy는 `/opt/stockmaster/app/data/artifacts/...`

동일 물리 파일을 세 경로로 가리킬 수 있다.

### 4.2 레이어별 해석 불일치

- `app/ui/helpers.py`
  - 자체 fallback resolver 존재
- `app/ml/inference.py`
  - `load_model_artifact(Path(str(training_run["artifact_uri"])))`
- `app/intraday/meta_inference.py`
  - same
- `app/intraday/meta_training.py`
  - same
- `app/release/validation.py`
  - `Path(...).exists()`
- `app/audit/checks.py`
  - `Path(...).exists()`

### 4.3 결과

- 화면은 열리는데 검증은 fail
- calibration은 되는데 inference는 fail
- cleanup 후 특정 기능만 깨짐

## 5. 리팩토링 원칙

### 5.1 공통 resolver 우선

모든 artifact path 해석은 단일 공통 helper를 거친다.

### 5.2 읽기 계층은 `경로 문자열`을 직접 해석하지 않는다

금지:

```python
Path(str(row["artifact_uri"])).exists()
load_model_artifact(Path(str(row["artifact_uri"])))
```

허용:

```python
resolved = resolve_artifact_path(settings, row["artifact_uri"])
if resolved is None:
    ...
payload = load_model_artifact(resolved)
```

### 5.3 UI 전용 resolver를 공통 resolver의 thin wrapper로 축소

UI helper 안의 특수 규칙이 별도 진실이 되면 안 된다.

### 5.4 validation / audit도 resolver를 사용한다

“실제로 열 수 있는가”를 기준으로 검증해야지, 저장된 raw path 문자열이 현재 프로세스 경로와 일치하는지를 기준으로 검증하면 안 된다.

## 6. 구현 범위

### 6.1 공통 모듈

신규 공통 모듈:

- `app/common/artifacts.py`

책임:

- candidate path 생성
- host/container/legacy artifact path 매핑
- 존재하는 실제 path resolve

### 6.2 1차 적용 대상

- `app/ui/helpers.py`
- `app/ml/inference.py`
- `app/ml/shadow.py`
- `app/ml/active.py`
- `app/intraday/meta_inference.py`
- `app/intraday/meta_training.py`
- `app/release/validation.py`
- `app/audit/checks.py`

### 6.3 2차 적용 대상

- report rendering / preview 경로
- future artifact-producing jobs

## 7. 경로 해석 규칙

resolver는 최소한 다음 케이스를 지원해야 한다.

### 7.1 현재 유효 absolute path

- 예: `/workspace/data/artifacts/...`

### 7.2 host runtime absolute path

- 예: `/opt/stockmaster/runtime/data/artifacts/...`

### 7.3 legacy app-local path

- 예: `/opt/stockmaster/app/data/artifacts/...`

### 7.4 relative path

- project root 기준 상대 경로
- artifacts root 기준 상대 경로

## 8. 구현 세부 요구사항

### 8.1 공통 resolver API

예상 API:

```python
artifact_candidate_paths(settings, path_value) -> list[Path]
resolve_artifact_path(settings, path_value, *, must_exist=True) -> Path | None
```

### 8.2 UI helper

- 기존 `resolve_ui_artifact_path()`는 공통 resolver wrapper로 유지 가능
- `_latest_manifest_preview()`도 공통 resolver 사용

### 8.3 alpha inference

- 실제 inference 직전에 artifact path를 resolve
- resolve 실패 시 proxy fallback 또는 no-op path로 진입

### 8.4 meta inference / meta calibration

- active meta model row가 있어도 artifact resolve 실패 시 `training_artifact_missing`
- calibration도 raw path가 아니라 resolved path를 기준으로 read/write

### 8.5 active model freeze

- artifact_uri 문자열이 있어도 실제 resolve 실패면 active 반영 후보로 쓰지 않는다

### 8.6 release validation / audit

- report artifact 존재 여부는 raw `Path.exists()`가 아니라 resolver 성공 여부 기준
- payload path도 동일

## 9. 테스트 요구사항

필수 테스트:

- legacy `project_root/data/artifacts/...` -> current artifacts root resolve
- host runtime absolute path -> current artifacts root resolve
- UI live recommendation path resolve 유지
- intraday console / market mood 기존 테스트 깨지지 않음

후보 테스트 파일:

- `tests/unit/test_stock_workbench_live.py`
- 신규 `tests/unit/test_artifact_resolver.py`
- 필요 시 release/audit 관련 단위 테스트 보강

## 10. 완료 기준

- artifact path resolver가 공통 모듈로 존재한다
- 핵심 read 경로가 raw `Path(str(...))` 직접 접근을 하지 않는다
- host/container/legacy 경로 차이 때문에 UI와 validation 결과가 갈리지 않는다
- cleanup 이후에도 active artifact 참조가 깨지지 않는다
- 관련 단위 테스트가 추가된다

## 11. 현재 상태

- UI helper에는 legacy fallback resolver가 이미 있음
- 이 티켓은 그 규칙을 공통화하고, 비-UI 레이어까지 같은 규칙을 적용하는 작업이다

