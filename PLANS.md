# FinTech SaaS Revenue Health Pipeline — Portfolio Project Plan

## Context

Shoaib's resume (`Resume/shoaib-rahaman-resume-v2.yaml`) and the reviewer feedback (`Resume/review_feedback.md`, `Resume/reviewer-issues.md`) identify two structural gaps that a public portfolio project can address:

1. **No public code** — a hiring committee cannot verify dbt/Python skills exist outside a private, single-employer environment.
2. **Airflow signals "monitoring," not "authorship"** — the resume lists "Apache Airflow / AWS MWAA (pipeline monitoring, failure triage)," which invites "walk me through a DAG you built from scratch" — a question the resume currently can't support.

This project is the first of a planned portfolio series (see `docs/roadmap.md` deliverable below), each chosen to pair a data domain with a warehouse/visualization combination that's new to Shoaib, building a coherent "I chose tools based on data shape and job-market fit" narrative across the portfolio.

**This project (v1):** An ELT pipeline analyzing quarterly **revenue health** (revenue, COGS, gross margin, QoQ/YoY growth) across US-domestic-filer holdings of the Global X FinTech ETF (FINX), sourced from SEC EDGAR's free XBRL `companyfacts` API, with a single-company deep dive on **Robinhood (HOOD)**.

**Key decisions and why:**
- **Domain: Fintech SaaS** — broad enough to avoid "cybersecurity-only" signaling (a real concern given the AE job market for cybersecurity specifically is narrow), while staying close enough to Shoaib's B2B SaaS revenue-analytics background to be fluent in interviews.
- **Universe: FINX holdings, filtered to US domestic 10-K/10-Q filers** — excludes foreign private issuers (Shopify, Adyen, Nubank, StoneCo, Wise, Temenos — 20-F/40-F filers with annual-only, inconsistent XBRL). This filter is itself a documented data-quality decision for the README.
- **Deep dive: Robinhood (HOOD)** — recognizable, US domestic filer, and has a genuinely interesting financial-health arc (meme-stock volatility → recent profitability) without requiring niche domain expertise.
- **v1 scope: Revenue health only** — the minimal income-statement concepts (`Revenues`, `CostOfRevenue`, `GrossProfit`) needed to ship an end-to-end pipeline fast. v2 (Rule of 40) and v3 (Magic Number) extend the same fact table additively — see roadmap doc.
- **Architecture: AWS lakehouse (S3 + Glue + Athena + dbt-athena/Iceberg)** — chosen over BigQuery/Snowflake because (a) raw XBRL data has real schema drift across companies/years, which a schema-on-read lakehouse handles naturally; (b) cost stays near-zero at this data volume; (c) synergizes with Shoaib's concurrent AWS coursework; (d) Snowflake is already used daily at work — diversification matters for new-role positioning.
- **Orchestration: self-hosted Airflow via Docker Compose** (not MWAA — no free tier, ~$350+/mo minimum). This is the piece that directly closes the "DAG authorship" gap.
- **Visualization: Tableau Public (broad index) + Streamlit/DuckDB (Robinhood deep dive)** — Tableau leverages existing expertise for a fast, polished, shareable dashboard; Streamlit is a new skill (Python data app) that demonstrates versatility and hosts the semantic-layer exploration.
- **Semantic layer: dbt MetricFlow, time-boxed evaluation** — a real, growing skill gap (similar to Airflow). If `dbt-athena` + MetricFlow integration proves too immature within ~1-2 days, fall back to documented mart columns + a simpler metric-picker UI, with the evaluation outcome documented either way.

**Future portfolio mapping** (to be written into `docs/roadmap.md` as part of this project):

| Project | Warehouse | Visualization | Rationale |
|---|---|---|---|
| **This project** — Fintech SaaS / EDGAR | AWS (S3+Glue+Athena, lakehouse) | Tableau Public + Streamlit | Schema-on-read fits XBRL drift; near-zero cost; AWS coursework synergy |
| Olist E-commerce (future) | Redshift | Power BI | Clean relational/star-schema data fits Redshift's Postgres lineage; Power BI diversifies BI tooling, common in retail/e-commerce |
| GitHub Archive (future) | BigQuery | Looker Studio | Native nested/repeated JSON support + GH Archive public BigQuery dataset; Looker Studio is BigQuery-native |
| Snowflake capstone (future) | Snowflake (deepen Snowpipe, dynamic tables, cost governance) | Apache Superset | Closes Snowflake skill gaps — Snowpipe/dynamic tables are low-priority (platform-team territory), cost governance is medium-priority (directly affects AE work and is already a resume claim) |

---

## Repo

New repo: `C:\Users\Graduate\Documents\GitHub\fintech-saas-revenue-pipeline\`

```
fintech-saas-revenue-pipeline/
├── README.md
├── .gitignore
├── .env.example
├── docs/
│   └── roadmap.md
├── ingestion/
│   ├── extract.py
│   ├── company_universe.py
│   ├── sec_client.py
│   ├── s3_writer.py
│   ├── utils.py
│   ├── requirements.txt
│   └── tests/
├── infra/
│   ├── glue/{crawler_config.json, database.sql}
│   ├── iam/{airflow_execution_policy.json, streamlit_readonly_policy.json}
│   └── s3/bucket_layout.md
├── orchestration/
│   ├── dags/edgar_revenue_pipeline_dag.py
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── config/airflow.env.example
├── dbt/
│   ├── dbt_project.yml, profiles.yml.example, packages.yml
│   ├── models/staging/   (4 models + sources/schema yml)
│   ├── models/intermediate/ (3 models)
│   ├── models/marts/     (dim_company, dim_date, fact_company_financials, fact_company_financials_export)
│   ├── macros/ (safe_divide, pick_first_non_null_tag, generate_schema_name)
│   ├── seeds/ (company_universe.csv, xbrl_concept_map.csv)
│   ├── tests/ (4 singular tests)
│   └── analyses/
├── semantic_layer/ (metrics.yml + README documenting MetricFlow decision)
├── export/export_marts_to_parquet.py
└── dashboard/
    ├── README.md
    ├── tableau/
    └── streamlit_app/ (app.py, pages/, data_access.py, requirements.txt)
```

---

## Ingestion Layer (`ingestion/`)

- **`company_universe.py`** (manual/on-demand, NOT in the daily DAG): FINX holdings → ticker→CIK via SEC's `company_tickers.json` → filter to companies whose recent filings are `10-K`/`10-Q` (via `data.sec.gov/submissions/CIK##########.json`), excluding `20-F`/`40-F`/`6-K`-dominated filers. Outputs `dbt/seeds/company_universe.csv` (cik, ticker, company_name, sic_code, filer_type, ipo_year, market_cap_tier). Document this as a deliberate "ETF composition isn't a daily-cadence concern" decision.
- **`sec_client.py`**: rate-limited (~8 req/sec, under SEC's 10/sec) HTTP client with mandatory `User-Agent` (from `SEC_USER_AGENT` env var, fail-fast if missing), exponential backoff retry (3x) on 429/5xx.
- **`extract.py`**: for each CIK, GET `data.sec.gov/api/xbrl/companyfacts/CIK{cik:0>10}.json`, write two outputs to S3:
  - `raw/edgar/companyfacts/cik={cik}/fetch_date={date}/full.json` (untouched, supports v2/v3 reuse)
  - `raw/edgar/income_statement_facts/cik={cik}/fetch_date={date}/facts.json` (flattened JSON Lines, filtered to `Revenues`/`RevenueFromContractWithCustomerExcludingAssessedTax`/`CostOfRevenue`/`CostOfGoodsAndServicesSold`/`GrossProfit`)
  - Error handling: 404 → log+skip (new IPOs w/o filings); persistent failure → append to `failed_ciks_{run_date}.json`, continue; DAG task fails only if >20% of companies fail.

---

## AWS Setup (`infra/`)

- **Buckets**: `srahaman-fintech-edgar-lakehouse` (raw/processed/Athena results) + **separate** `srahaman-fintech-edgar-public` (Parquet export only, public-read) — keeps the public-access surface trivially auditable.
- **Glue**: database `edgar_fintech_raw`, one crawler `edgar_income_statement_crawler` targeting the flattened facts prefix, **on-demand only** (triggered by Airflow via boto3, polled until `READY`) — no scheduled/always-on cost.
- **Athena**: dedicated workgroup `edgar_fintech_wg`, per-query scan limit (cost guardrail), engine v3 (Iceberg support for dbt-athena incrementals).
- **IAM**: Airflow execution role scoped to only the specific bucket/crawler/workgroup ARNs needed (no `CreateCrawler`, no IAM perms, no other-bucket access). Streamlit needs **zero** AWS credentials — DuckDB's `httpfs` reads the public export bucket directly.

---

## dbt Project (`dbt/`)

**Staging** (`models/staging/`): `stg_edgar__company_facts` (base cast/rename/dedupe via `qualify row_number()` on accession number for amended filings), `stg_edgar__revenues`, `stg_edgar__cost_of_revenue`, `stg_edgar__gross_profit` — each filters to relevant XBRL concepts using a tag-priority macro.

**Intermediate** (`models/intermediate/`): `int_company_facts__unioned` (full-outer-join/coalesce across staging models on `cik, period_end_date, fiscal_period, form_type` — documented as outer join because not every company tags every concept every period), `int_revenue_health__by_period` (computes `gross_profit_final = coalesce(reported, revenue - cost_of_revenue)`, `gross_margin_pct` via `safe_divide`), `int_revenue_health__growth_calcs` (window functions for QoQ/YoY growth, with documented fallback for irregular fiscal calendars).

**Marts** (`models/marts/`):
- `dim_company` — from seed, joined to latest EDGAR `entity_name`; includes `ipo_year`, `market_cap_tier`, `cohort` (built now so v2/v3 marts join on `cik` for free).
- `dim_date` — fiscal period spine.
- `fact_company_financials` — grain `(cik, period_end_date, form_type)`. **Incremental + Iceberg** (`incremental_strategy='merge'`, unique key on grain) — the project's "incremental over full refresh" model, justified by continuously-arriving quarterly filings + occasional amendments.
- `fact_company_financials_export` — plain `table`, thin select for Parquet export (decouples export artifact from Iceberg internals).

**Macros** (`macros/`): `safe_divide` (null/zero-safe ratio), `pick_first_non_null_tag` (XBRL tag-fallback, driven by `seeds/xbrl_concept_map.csv` — reusable for v2/v3), `generate_schema_name` (dev/prod Athena database separation).

**Custom singular tests** (`tests/`, 4 total):
1. `assert_gross_profit_lte_revenue.sql` — flags `gross_profit > revenue`.
2. `assert_revenue_non_negative.sql` — flags `revenue < 0`.
3. `assert_period_end_date_sequential.sql` — per-company, per-form_type, each period's `period_end_date` must exceed the prior one (catches dedup/amendment issues).
4. `assert_gross_margin_within_bounds.sql` — flags `gross_margin_pct` outside `[-1.0, 1.0]`.

Plus generic tests: `not_null`/`unique` on grain, `relationships` to `dim_company`, `accepted_values` on `form_type`/`fiscal_quarter`.

---

## Semantic Layer (`semantic_layer/`)

Time-boxed (~1-2 days) evaluation of dbt MetricFlow over `fact_company_financials`: entity `company` (cik), dimensions (`period_end_date`, `fiscal_quarter`, `cohort`, `ticker`), measures (`revenue`, `cost_of_revenue`, `gross_profit`), metrics (`total_revenue`, `gross_margin_pct`, `revenue_qoq_growth`, `revenue_yoy_growth`).

**Fallback trigger**: `dbt-athena-community` doesn't support `dbt sl query` against Athena/Iceberg, or MetricFlow's time-spine conflicts with irregular fiscal-quarter-ends across companies. **Fallback**: defer to v2; v1 ships with documented mart columns + a simpler "Explore Metrics" dropdown in Streamlit. Document the outcome either way in `semantic_layer/README.md` — this is itself a good interview talking point regardless of outcome.

---

## Airflow DAG (`orchestration/`)

Single DAG `edgar_revenue_health_pipeline`, 8 tasks:

```
extract_companyfacts >> land_raw_s3_check >> trigger_glue_crawler
  >> dbt_seed >> dbt_run_staging >> dbt_run_intermediate_marts
  >> dbt_test >> dbt_docs_generate >> export_parquet_snapshots
```

- `default_args`: `retries=2`, exponential backoff, `max_retry_delay=20min`, `email_on_failure=True`.
- `schedule_interval="0 7 * * 2-6"` (weekday mornings UTC — EDGAR filings are business-day events), `catchup=False` (current-state pipeline, not historical replay), `max_active_runs=1` (Glue crawlers can't run concurrently on the same target).
- Docker Compose: Postgres (Airflow metadata) + airflow-init + webserver + scheduler, **LocalExecutor** (sufficient for single-DAG project — document as deliberate scope decision). Custom Dockerfile installs `dbt-athena-community`, `boto3`, `apache-airflow-providers-amazon` (consider `GlueCrawlerOperator` over hand-rolled boto3 calls).

---

## Dashboards (`dashboard/`)

**Tableau Public** (broad index): Parquet-extract-based (no live AWS dependency for a published-once workbook). Sheets: Revenue Trend Overview (all companies, HOOD highlighted), Gross Margin Comparison (heatmap), Growth Leaders/Laggards (ranked YoY growth), Company Detail (parameter-driven single-company view).

**Streamlit + DuckDB** (Robinhood deep dive), multipage:
1. `1_Revenue_Trend.py` — HOOD revenue/COGS/gross profit/margin trends; note v1 can't yet show the transaction-vs-net-interest-income mix shift (needs segment-level concepts — flag as v2+ extension).
2. `2_Peer_Comparison.py` — HOOD's metrics vs. percentile distribution across the universe (DuckDB `percent_rank()`).
3. `3_Ask_Semantic_Layer.py` — MetricFlow-powered metric/dimension picker, or fallback "Explore Metrics" dropdown per the semantic layer decision above.

`data_access.py` reads Parquet directly from the public S3 bucket via DuckDB `httpfs` (zero credentials) — fallback option: bundle a local Parquet snapshot for fully offline operation if live S3 reads prove unreliable for graders.

---

## `docs/roadmap.md` (deliverable)

Sections: (1) this project's v2 (Rule of 40 — add operating/free cash flow concepts, new `int_revenue_health__rule_of_40` model, additive to `fact_company_financials`) and v3 (Magic Number — add S&M expense concepts, same additive pattern); (2) Olist/Redshift/Power BI; (3) GitHub Archive/BigQuery/Looker Studio; (4) Snowflake capstone (Snowpipe + dynamic tables = low priority, cost governance = medium priority)/Apache Superset; (5) skill-gap coverage matrix mapping each project to the resume gap it closes.

---

## README (deliverable)

Sections per `data_project_guide.md`: Architecture diagram, Data Flow, Key Design Decisions (FINX/domestic-filer filter, incremental+Iceberg rationale, DAG schedule rationale, XBRL tag-fallback strategy, custom test rationale, MetricFlow decision, Parquet export decoupling), Stack table, How to Run, Dashboards (links/screenshots), dbt Docs link (GitHub Pages), Roadmap (link to `docs/roadmap.md`).

---

## Build Sequence (10 phases, ~6-10 weeks evenings/weekends)

0. **Foundations** — repo scaffold, README/roadmap skeletons, `company_universe.py` → seed CSV.
1. **Minimal ingestion** — `sec_client.py` + `extract.py` for 3-5 companies (incl. HOOD) → S3.
2. **AWS cataloging** — Glue DB/crawler, validate Athena query against small subset.
3. **dbt skeleton** — staging models + schema tests passing on small subset.
4. **Intermediate + marts** — full v1 model set, macros, incremental/Iceberg fact table.
5. **Custom tests** — 4 singular tests, validated against intentionally-broken sample data.
   *(Phases 0-5 = working small-scale end-to-end pipeline, ~2 weeks — first demoable milestone.)*
6. **Scale ingestion** — full ~30-50 company universe, re-run Glue/dbt, fix tag-drift edge cases.
7. **Airflow DAG** — Docker Compose, full DAG wired end-to-end, validate retry behavior.
8. **Export + dashboards** — Parquet export script + public bucket policy, Tableau Public publish, Streamlit app.
9. **MetricFlow evaluation** — time-boxed, document outcome (parallelizable with Phase 8).
10. **Documentation + polish** — dbt docs → GitHub Pages, README/roadmap finalization, git history cleanup into a story-telling commit sequence.

---

## Verification

- **Phase 1-5 milestone**: `dbt test` passes on the 3-5 company subset; manually inspect `fact_company_financials` rows for HOOD against its actual 10-Q filings to sanity-check tag mapping.
- **Custom tests**: temporarily inject a bad row (e.g., `gross_profit > revenue`) into a dev table and confirm each singular test fails as expected, then revert.
- **Phase 6**: confirm Glue crawler picks up schema variations across companies without manual intervention; spot-check 3-4 non-HOOD companies' computed gross margins against their filings.
- **Phase 7**: trigger a deliberate failure (e.g., bad AWS credential) to confirm Airflow retry/backoff and email-on-failure behavior; confirm `max_active_runs=1` prevents overlapping Glue crawler triggers.
- **Phase 8**: load the published Tableau Public dashboard and the deployed Streamlit app from a clean browser/session (no local AWS creds) to confirm both work for an external viewer (recruiter simulation).
- **Final**: fresh clone + `README.md` "How to Run" instructions on a clean checkout — confirm someone else could reproduce the local pipeline.
