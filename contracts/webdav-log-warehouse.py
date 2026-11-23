from datetime import date

import polars as pl


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


def _date_value(column: str) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=True)
        .str.strip_chars()
    )

    parsed_datetime = (
        value.str.strptime(
            pl.Datetime,
            format="%Y-%m-%d %H:%M:%S",
            strict=False,
        )
        .dt.date()
    )

    parsed_date = value.str.strptime(
        pl.Date,
        format="%Y-%m-%d",
        strict=False,
    )

    excel_serial = (
        pl.lit(date(1899, 12, 30))
        + pl.duration(
            days=value.cast(pl.Int64, strict=False)
        )
    ).cast(pl.Date)

    return pl.coalesce(
        [
            parsed_datetime,
            parsed_date,
            excel_serial,
        ]
    )


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
        _date_value("Date").alias("transaction_date"),

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
        _date_value("Date").alias("transaction_date"),

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
    snapshot_glob = (
        f"r2://{bucket}/"
        f"{INCOMING_SNAPSHOT_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {INCOMING_TABLE_NAME} AS
        SELECT
            transaction_date,
            source,
            transaction_type,
            product_type,
            pounds,
            note
        FROM (
            SELECT
                transaction_date,
                source,
                transaction_type,
                product_type,
                pounds,
                note,
                filename,
                MAX(filename) OVER () AS latest_filename
            FROM read_parquet(
                {sql_string(snapshot_glob)},
                filename = true
            )
        )
        WHERE filename = latest_filename
        """
    )

    return require_nonempty_table(
        con,
        INCOMING_TABLE_NAME,
    )


def load_outgoing(
    con,
    bucket: str,
    sql_string,
    require_nonempty_table,
) -> int:
    snapshot_glob = (
        f"r2://{bucket}/"
        f"{OUTGOING_SNAPSHOT_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {OUTGOING_TABLE_NAME} AS
        SELECT
            transaction_date,
            product_type,
            pounds,
            note
        FROM (
            SELECT
                transaction_date,
                product_type,
                pounds,
                note,
                filename,
                MAX(filename) OVER () AS latest_filename
            FROM read_parquet(
                {sql_string(snapshot_glob)},
                filename = true
            )
        )
        WHERE filename = latest_filename
        """
    )

    return require_nonempty_table(
        con,
        OUTGOING_TABLE_NAME,
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
