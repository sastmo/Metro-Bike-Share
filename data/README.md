# Data Layout

The data area is organized to support a warehouse-style project structure instead of a flat analysis folder.

## Folders

- `data/raw/trips/`: quarterly Metro Bike Share trip exports used as landing-zone inputs
- `data/raw/stations/`: station snapshots and reference station extracts
- `data/reference/census/`: demographic reference inputs
- `data/reference/geography/`: geographic reference inputs
- `data/reference/transport/`: transit-related reference inputs

## Working assumptions

- These files are treated as local working data, not source-controlled artifacts.
- The project is being modernized before a full data-quality reconciliation pass.
- Some legacy SQL expects additional external files that are not yet packaged in this repository. Those dependencies are called out in the manifest and runbook so the project remains honest about current scope.

