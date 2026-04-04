# Pipeline Runbook

This runbook documents the intended warehouse-oriented execution sequence for the preserved SQL assets.

## Recommended order

1. Foundation
2. Staging
3. Feature engineering
4. Marts
5. Enrichment

## Layer details

### Foundation

Start with `sql/legacy/foundation/metro_bike_share.sql` to load the base trip tables, helper functions, and standardized trip structures.

### Staging

Run the quarter-level scripts under `sql/legacy/staging/` to clean and standardize each trip batch.

### Feature engineering

Run the clustering and classification scripts under `sql/legacy/features/`. The shared point helper logic now lives in `sql/warehouse/utilities/points.sql`.

### Marts

Use the scripts under `sql/legacy/marts/` to assemble reporting and analysis-ready views.

### Enrichment

Finish with `sql/legacy/enrichment/` if the external reference inputs are available locally.

## Practical note

The repo has been cleaned and documented, but not every legacy dependency has been fully normalized yet. That is an intentional part of the phase 1 scope: improve structure first, then deepen reproducibility and source governance in the next pass.

