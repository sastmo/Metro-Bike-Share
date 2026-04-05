# Legacy SQL

This directory preserves the original SQL work while giving it a clearer structure.

The intent is not to hide or discard the earlier implementation. The intent is to keep the original depth visible while making the project easier to explain:

- `foundation/` contains bootstrap and shared cleaning logic
- `staging/` contains quarter-level trip cleaning
- `features/` contains classification and clustering work
- `marts/` contains analysis-ready views
- `enrichment/` contains demographic and transportation joins

Some scripts still reflect historical assumptions and old naming conventions. They remain here intentionally as preserved project history while the new `sql/warehouse/` layer grows around them.

