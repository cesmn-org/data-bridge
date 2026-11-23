"""Shared DuckDB/MotherDuck helpers for lakehouse loading contracts."""


def quote_identifier(value: str) -> str:
    """Quote a trusted DuckDB identifier."""
    return '"' + value.replace('"', '""') + '"'


def load_latest_snapshot(
    con,
    *,
    bucket: str,
    snapshot_prefix: str,
    table_name: str,
    columns: list[str],
    sql_string,
    require_nonempty_table,
) -> int:
    """Replace a table with the contents of its newest R2 Parquet snapshot."""
    if not columns:
        raise ValueError("At least one output column is required")

    snapshot_glob = f"r2://{bucket}/{snapshot_prefix}*.parquet"
    selected_columns = ",\n            ".join(
        quote_identifier(column) for column in columns
    )
    table = quote_identifier(table_name)

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
            {selected_columns}
        FROM read_parquet(
            {sql_string(snapshot_glob)},
            filename = true
        )
        WHERE filename = (
            SELECT MAX(filename)
            FROM read_parquet(
                {sql_string(snapshot_glob)},
                filename = true
            )
        )
        """
    )

    return require_nonempty_table(con, table_name)
