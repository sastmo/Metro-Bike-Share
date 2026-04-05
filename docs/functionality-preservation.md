# Functionality Preservation Map

This note exists to make one point clear: the repository reorganization did not intentionally remove the original analytical SQL logic. The goal was to relocate and frame it better, not to throw away the work.

## Preserved capabilities

### Base ingestion, cleaning, and helper functions

Current location:

- `sql/legacy/foundation/metro_bike_share.sql`

What is preserved:

- base raw trip and station table definitions
- typed cleaned trip structure
- validation and casting helper functions
- trip cleaning logic for the initial yearly pipeline

Examples:

- type definitions at `sql/legacy/foundation/metro_bike_share.sql`
- helper functions like `check_id`, `Check_date`, `check_duration`, `check_station`, `check_b_ids`, `check_lat_lon`, `check_plan_duration`, and `check_text`

### Quarter-level staging logic

Current location:

- `sql/legacy/staging/`

What is preserved:

- quarter-by-quarter CTE-based cleaning pipelines
- trip normalization logic
- filtering rules for invalid coordinates, testing rows, and plan anomalies

### SQL K-means feature engineering

Current location:

- `sql/legacy/features/Processing-duration-class.sql`
- `sql/legacy/features/Processing-station-class.sql`
- year-specific variants under the same folder

What is preserved:

- recursive SQL K-means implementation
- cluster assignment logic
- centroid updates using point averages
- WCSS evaluation comments and chosen cluster counts

What changed:

- the old missing `points.sql` dependency was replaced with a repo-local include:
  `sql/warehouse/utilities/points.sql`

### Analysis marts and visualization-ready views

Current location:

- `sql/legacy/marts/`

What is preserved:

- CTE-heavy yearly view construction
- joins between cleaned trips, station details, and classification outputs
- cube-based aggregations
- analysis-ready views for downstream visualization work

### Demographic and transit enrichment

Current location:

- `sql/legacy/enrichment/`

What is preserved:

- population-by-zip logic
- public transportation station parsing and integration logic
- JSON processing and nearest-zip matching logic

## What actually changed during reorganization

- files were moved into a layered structure
- the K-means scripts now include a repo-local `points.sql`
- the main bootstrap SQL now points to local repo data folders instead of a historical personal path
- documentation, validation scripts, and tests were added around the original SQL assets

## Known caveats

- not every legacy SQL file has been executed end-to-end after the restructure yet
- some enrichment scripts still reference older external source paths and need a follow-up normalization pass
- the reorganization preserved logic structure, but full runtime verification should be a separate smoke-test task

