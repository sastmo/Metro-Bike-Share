# Metro Bike Share Warehouse

SQL-first mobility analytics project built around PostgreSQL, warehouse-style data modeling, and database-side feature engineering.

This repository started as an older analysis project. It has now been restructured into a cleaner portfolio project that is easier to explain, maintain, and extend. The current phase is focused on repository modernization, project structure, documentation, and SQL asset organization. Predictive modeling and time-series forecasting are intentionally deferred to phase 2.

## Why this project is strong for a portfolio

- It keeps heavy transformation logic inside PostgreSQL instead of moving large tables back and forth between the database and Python.
- It shows database-centric feature engineering, including SQL-based trip cleaning, dimension enrichment, and K-means style clustering executed close to the data.
- It is positioned as a data engineering and analytics engineering project, not only a dashboard or notebook exercise.
- It now includes project contracts, repository validation, and basic automated tests so the codebase feels more production-minded.

## Current project focus

Phase 1 is about making the repository clean, credible, and extensible:

- reorganize the file tree around data warehouse layers
- preserve the original SQL work without leaving the repo flat and difficult to navigate
- make the project easier to present to employers as a SQL-first engineering case study
- create space for a second phase focused on forecasting, prediction, and deeper data quality work

At this stage, the goal is not to claim that every source file is perfect. The goal is to present a stronger technical foundation and a clearer architecture.

## Repository layout

```text
.
├── README.md
├── Makefile
├── data
│   ├── README.md
│   ├── raw
│   │   ├── stations
│   │   └── trips
│   └── reference
│       ├── census
│       ├── geography
│       └── transport
├── docs
│   ├── architecture.md
│   └── legacy-inventory.md
├── scripts
│   └── validate_repo.py
├── sql
│   ├── legacy
│   │   ├── foundation
│   │   ├── staging
│   │   ├── features
│   │   ├── marts
│   │   └── enrichment
│   └── warehouse
│       ├── contracts
│       ├── orchestration
│       └── utilities
└── tests
    └── test_repository_contract.py
```

## Architecture summary

The repository now tells a clearer warehouse story:

1. Raw data lands in `data/raw/` and reference inputs live in `data/reference/`.
2. Legacy SQL has been preserved under `sql/legacy/`, grouped by layer instead of left flat at the repo root.
3. Reusable warehouse assets live under `sql/warehouse/`, including contracts, utilities, and runbooks.
4. Documentation under `docs/` explains the technical positioning and future roadmap.
5. Validation and unit tests help protect the repository structure as the project grows.

## What makes this project different technically

- SQL-first design: the project emphasizes pushing transformations and feature generation down into PostgreSQL.
- Warehouse framing: raw inputs, staging logic, feature engineering, marts, and enrichment are separated conceptually.
- Database-side clustering: the K-means scripts demonstrate an uncommon but valuable database-centric approach.
- Portfolio positioning: this is closer to analytics engineering, data warehousing, and data platform work than a simple one-off analysis.

## Key repository assets

- `sql/legacy/`: preserved original SQL, now grouped into logical layers
- `sql/warehouse/contracts/source_manifest.json`: high-level inventory of source domains and warehouse assets
- `sql/warehouse/utilities/points.sql`: reusable point helper functions used by the clustering logic
- `sql/warehouse/orchestration/pipeline_runbook.md`: practical run order and modernization notes
- `scripts/validate_repo.py`: repository contract validation
- `tests/test_repository_contract.py`: lightweight automated tests

## Validation

```bash
python3 scripts/validate_repo.py
python3 -m unittest discover -s tests -p 'test_*.py'
```

## Project positioning

This project is best presented as:

- a PostgreSQL-first mobility warehouse case study
- an analytics engineering project with strong SQL depth
- a data engineering portfolio piece that intentionally minimizes unnecessary database-to-Python data movement

That positioning is important. The value here is not only the final insights. The value is the database design mindset, the operational structure, and the ability to engineer analysis-ready data products inside the warehouse layer.

## Phase 2 roadmap

The next phase can build on this structure without redoing the repository again:

- data source reconciliation and stronger data quality checks
- canonical warehouse schemas and standardized naming conventions across all SQL assets
- time-series forecasting and prediction pipelines
- BI-facing marts or semantic models for reporting tools
- optional orchestration around reproducible database runs

## Notes

- Large raw and reference datasets are kept local by default and ignored from Git through `.gitignore`.
- Some legacy enrichment scripts still depend on external inputs that are not yet packaged in the repository.
- The original SQL work is preserved intentionally so the project still shows the depth of the first implementation.

