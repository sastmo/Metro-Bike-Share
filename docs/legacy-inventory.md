# Legacy Inventory

The original SQL files were preserved and moved into logical layers so the repo is easier to navigate.

## Foundation

- `sql/legacy/foundation/metro_bike_share.sql`

Contains the original bootstrap logic, type definitions, cleaning functions, and the first major trip loading workflow.

## Staging

- `sql/legacy/staging/metro_bike_share-19-q2.sql`
- `sql/legacy/staging/metro_bike_share-19-q3.sql`
- `sql/legacy/staging/metro_bike_share-19-q4.sql`
- `sql/legacy/staging/metro_bike_share-20-q1.sql`
- `sql/legacy/staging/metro_bike_share-20-q2.sql`
- `sql/legacy/staging/metro_bike_share-20-q3.sql`
- `sql/legacy/staging/metro_bike_share-20-q4.sql`
- `sql/legacy/staging/metro_bike_share-21-q1.sql`
- `sql/legacy/staging/metro_bike_share-21-q2.sql`
- `sql/legacy/staging/metro_bike_share-21-q3.sql`
- `sql/legacy/staging/metro_bike_share-21-q4.sql`
- `sql/legacy/staging/metro_bike_share-22-q1.sql`
- `sql/legacy/staging/metro_bike_share-22-q2.sql`
- `sql/legacy/staging/metro_bike_share-22-q3.sql`

These scripts preserve quarter-specific cleaning and normalization work.

## Features

- `sql/legacy/features/Processing-duration-class.sql`
- `sql/legacy/features/Processing-duration-class-20.sql`
- `sql/legacy/features/Processing-duration-class-21.sql`
- `sql/legacy/features/Processing-duration-class-22.sql`
- `sql/legacy/features/Processing-station-class.sql`
- `sql/legacy/features/Processing-station-class-20.sql`
- `sql/legacy/features/Processing-station-class-21.sql`
- `sql/legacy/features/Processing-station-class-22.sql`

This layer contains the database-side classification logic, including the K-means-oriented clustering work that makes the project stand out.

## Marts

- `sql/legacy/marts/Preparation-Analysis.sql`
- `sql/legacy/marts/Preparation-Analysis-20.sql`
- `sql/legacy/marts/Preparation-Analysis-21.sql`
- `sql/legacy/marts/Preparation-Analysis-22.sql`

These scripts combine cleaned trips, station details, and feature outputs into analysis-ready views.

## Enrichment

- `sql/legacy/enrichment/Population_zipe_code.sql`
- `sql/legacy/enrichment/Public_transportation_stations.sql`

This layer captures demographic and public transportation enrichment work. Some dependencies in this area still need a later normalization pass.

