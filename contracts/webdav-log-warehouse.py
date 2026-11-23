import polars as pl

from contracts.common import excel_compatible_date
from lakehouse.common import load_latest_snapshot


RAW_INCOMING_COLUMNS = [
    "Date",
    "Source",
    "Transaction Type",
    "Product Type",
    "Pounds",
    "Note",
]

RAW_OUTGOING_COLUMNS = [
    "Date",
    "Product Type",
    "Pounds",
    "Note",
]


INCOMING_OUTPUT_SCHEMA = pl.Schema(
    {
        "transaction_date": pl.Date,
        "source": pl.String,
        "transaction_type": pl.String,
        "product_type": pl.String,
        "pounds": pl.Int64,
        "note": pl.String,
    }
)


OUTGOING_OUTPUT_SCHEMA = pl.Schema(
    {
        "transaction_date": pl.Date,
        "product_type": pl.String,
        "pounds": pl.Int64,
        "note": pl.String,
    }
)


# Lakehouse loading contract

INCOMING_SNAPSHOT_PREFIX = (
    "snapshots/webdav-log-warehouse-incoming/"
)

OUTGOING_SNAPSHOT_PREFIX = (
    "snapshots/webdav-log-warehouse-outgoing/"
)

INCOMING_TABLE_NAME = "log_warehouse_incoming"
OUTGOING_TABLE_NAME = "log_warehouse_outgoing"


def _nullable_integer(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=True)
        .str.strip_chars()
        .replace("", None)
        .cast(pl.Int64, strict=True)
    )


def _nullable_string(column: str) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=True)
        .str.strip_chars()
    )

    return (
        pl.when(value == "")
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(value)
    )


def transform_incoming(
    df: pl.DataFrame,
) -> pl.DataFrame:
    return df.select(
        excel_compatible_date("Date").alias("transaction_date"),

        _nullable_string("Source").alias("source"),

        _nullable_string("Transaction Type")
        .alias("transaction_type"),

        _nullable_string("Product Type")
        .alias("product_type"),

        _nullable_integer("Pounds").alias("pounds"),

        _nullable_string("Note").alias("note"),
    )


def transform_outgoing(
    df: pl.DataFrame,
) -> pl.DataFrame:
    return df.select(
        excel_compatible_date("Date").alias("transaction_date"),

        _nullable_string("Product Type")
        .alias("product_type"),

        _nullable_integer("Pounds").alias("pounds"),

        _nullable_string("Note").alias("note"),
    )


def load_incoming(
    con,
    bucket: str,
    sql_string,
    require_nonempty_table,
) -> int:
    return load_latest_snapshot(
        con,
        bucket=bucket,
        snapshot_prefix=INCOMING_SNAPSHOT_PREFIX,
        table_name=INCOMING_TABLE_NAME,
        columns=INCOMING_OUTPUT_SCHEMA.names(),
        sql_string=sql_string,
        require_nonempty_table=require_nonempty_table,
    )


def load_outgoing(
    con,
    bucket: str,
    sql_string,
    require_nonempty_table,
) -> int:
    return load_latest_snapshot(
        con,
        bucket=bucket,
        snapshot_prefix=OUTGOING_SNAPSHOT_PREFIX,
        table_name=OUTGOING_TABLE_NAME,
        columns=OUTGOING_OUTPUT_SCHEMA.names(),
        sql_string=sql_string,
        require_nonempty_table=require_nonempty_table,
    )


def sample_incoming(con) -> list[tuple]:
    return con.execute(
        f"""
        SELECT
            transaction_date,
            source,
            transaction_type,
            product_type,
            pounds,
            note
        FROM {INCOMING_TABLE_NAME}
        ORDER BY transaction_date, source
        LIMIT 5
        """
    ).fetchall()


def sample_outgoing(con) -> list[tuple]:
    return con.execute(
        f"""
        SELECT
            transaction_date,
            product_type,
            pounds,
            note
        FROM {OUTGOING_TABLE_NAME}
        ORDER BY transaction_date, product_type
        LIMIT 5
        """
    ).fetchall()
