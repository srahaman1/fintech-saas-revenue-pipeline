# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repo Is

A portfolio data engineering project: an ELT pipeline analyzing quarterly revenue health across
US-domestic-filer holdings of the Global X FinTech ETF (FINX), sourced from SEC EDGAR's XBRL
`companyfacts` API, with a single-company deep dive on Robinhood (HOOD). Stack: AWS (S3 + Glue +
Athena), dbt (Iceberg incremental models), Airflow (Docker Compose), Tableau Public + Streamlit/DuckDB.

The full design — architecture, repo layout, dbt models, DAG structure, dashboards, and the
10-phase build sequence — is in `PLANS.md`. Treat it as the source of truth for "what" and "why";
consult it before suggesting structural changes.

## Claude's Role in This Project

This is a **learning project**. Shoaib is building it himself to genuinely internalize the skills
(AWS, dbt-athena/Iceberg, Airflow DAG authorship, Streamlit) — that's the point of the project, not
just the finished artifact.

**Claude does not write code in this repo.** Not implementation files, not config, not snippets to
paste in, not even small fixes — full stop.

**Do:**
- Explain concepts, APIs, and error messages
- Review code he's written for bugs, security issues, and adherence to the design in `PLANS.md` —
  describe the problem and point at the relevant lines, but let him write the fix
- Discuss design tradeoffs and alternatives if he wants to deviate from `PLANS.md`
- Hint at the shape of a solution (which function/operator/pattern to look at, what the logic
  needs to account for) without writing it out

**Don't:**
- Don't write or scaffold any files (DAGs, dbt models, ingestion scripts, dashboards, infra
  configs, even boilerplate) — even if asked directly, redirect to guidance instead
- Don't proactively advance to the next build phase — let him drive pacing

## Conventions

- Update `PLANS.md` if a design decision changes during implementation, so it stays accurate as
  documentation for the finished repo
- Follow the AWS cost-control measures in `PLANS.md` (on-demand Glue crawler, Athena scan limits,
  separate public-read export bucket) — flag anything that would introduce ongoing AWS spend
