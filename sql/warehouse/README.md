# Warehouse Layer

This directory contains the new project assets added during the modernization pass.

## Purpose

The warehouse layer is the bridge between the preserved legacy SQL and a more reusable data engineering project structure.

## Contents

- `contracts/`: inventories, manifests, and repository-level expectations
- `orchestration/`: runbooks and execution notes
- `utilities/`: reusable SQL helpers shared by multiple scripts

## Current state

Phase 1 is intentionally light-touch. The goal is to improve project structure and presentation without pretending the repository has already been fully rewritten into a brand-new platform.

Phase 2 can build from this layer by adding canonical schemas, repeatable orchestration, and stricter data tests.

