# KRX Service Registry

승인 완료 후 StockMaster에서 사용할 canonical KRX service registry입니다.

| service_slug | 서비스명 | category | endpoint_url | request field | expected usage |
| --- | --- | --- | --- | --- | --- |
| `stock_kospi_daily_trade` | 유가증권 일별매매정보 | stock_daily_trade | `https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd` | `basDd` | 유가증권 일별 거래통계 / 종목 보강 |
| `stock_kosdaq_daily_trade` | 코스닥 일별매매정보 | stock_daily_trade | `https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd` | `basDd` | 코스닥 일별 거래통계 / 종목 보강 |
| `stock_kospi_symbol_master` | 유가증권 종목기본정보 | symbol_master | `https://data-dbg.krx.co.kr/svc/apis/sto/stk_isu_base_info` | `basDd` | 유가증권 종목 기본정보 / reference enrichment |
| `stock_kosdaq_symbol_master` | 코스닥 종목기본정보 | symbol_master | `https://data-dbg.krx.co.kr/svc/apis/sto/ksq_isu_base_info` | `basDd` | 코스닥 종목 기본정보 / reference enrichment |
| `index_krx_daily` | KRX 시리즈 일별시세정보 | index_daily | `https://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd` | `basDd` | KRX 지수 시세 / market pulse |
| `index_kospi_daily` | KOSPI 시리즈 일별시세정보 | index_daily | `https://data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd` | `basDd` | KOSPI 지수 시세 / market pulse |
| `index_kosdaq_daily` | KOSDAQ 시리즈 일별시세정보 | index_daily | `https://data-dbg.krx.co.kr/svc/apis/idx/kosdaq_dd_trd` | `basDd` | KOSDAQ 지수 시세 / market pulse |
| `etf_daily_trade` | ETF 일별매매정보 | etf_daily | `https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd` | `basDd` | ETF 일별 매매정보 / ETF 통계 |

## 사용 규칙

- 승인된 slug만 `KRX_ALLOWED_SERVICES`에 넣는다
- `ENABLE_KRX_LIVE=true`여도 allowlist 밖 서비스는 호출되지 않는다
- 현재 모든 서비스는 `basDd=YYYYMMDD` 단일 기준일 파라미터를 사용한다

## 저장 계약 연결

아래 저장 계약이 KRX live 운영 추적에 사용된다.

- `fact_external_api_request_log`
- `fact_external_api_budget_snapshot`
- `fact_krx_service_status`
- `fact_source_attribution_snapshot`

## smoke test 예시

```powershell
python scripts/krx_smoke_test.py --service-slug etf_daily_trade --as-of-date 2026-03-06
python scripts/krx_smoke_test_all_allowed.py --as-of-date 2026-03-06
```
