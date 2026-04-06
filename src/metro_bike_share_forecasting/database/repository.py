from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from metro_bike_share_forecasting.database.schema import build_metadata

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.dialects.postgresql import insert
except ModuleNotFoundError:  # pragma: no cover - optional until dependencies are installed
    create_engine = text = insert = None


if create_engine is None:  # pragma: no cover - dependency guard
    class PostgresRepository:  # type: ignore[no-redef]
        def __init__(self, *_args, **_kwargs) -> None:
            raise ModuleNotFoundError("sqlalchemy and psycopg2 are required for PostgreSQL persistence.")

else:
    class PostgresRepository:
        def __init__(self, postgres_url: str, schema_name: str) -> None:
            self.engine = create_engine(postgres_url, future=True)
            self.schema_name = schema_name
            self.metadata = build_metadata(schema_name)

        def initialize_schema(self, sql_schema_path: Path) -> None:
            sql_text = sql_schema_path.read_text()
            with self.engine.begin() as connection:
                for statement in sql_text.split(";"):
                    stripped = statement.strip()
                    if stripped:
                        connection.execute(text(stripped))

        def upsert_dataframe(self, table_name: str, frame: pd.DataFrame, conflict_columns: Iterable[str]) -> None:
            if frame.empty:
                return
            table = self.metadata.tables[f"{self.schema_name}.{table_name}"]
            records = frame.where(pd.notnull(frame), None).to_dict(orient="records")
            conflict_columns = list(conflict_columns)

            with self.engine.begin() as connection:
                for start in range(0, len(records), 1000):
                    batch = records[start : start + 1000]
                    statement = insert(table).values(batch)
                    update_columns = {
                        column.name: statement.excluded[column.name]
                        for column in table.columns
                        if column.name not in conflict_columns
                    }
                    connection.execute(
                        statement.on_conflict_do_update(
                            index_elements=conflict_columns,
                            set_=update_columns,
                        )
                    )
