import polars as pl

from contracts.common import excel_compatible_date
from lakehouse.common import load_latest_snapshot


RAW_COLUMNS = [
    "Date",
    "Location",
    "Bakery",
    "Dairy",
    "Deli",
    "Dry",
    "Frozen",
    "Nonfood",
    "Produce",
    "Total",
]


OUTPUT_SCHEMA = pl.Schema(
    {
        "rescue_date": pl.Date,
        "location": pl.String,
        "bakery": pl.Int64,
        "dairy": pl.Int64,
        "deli": pl.Int64,
        "dry": pl.Int64,
        "frozen": pl.Int64,
        "nonfood": pl.Int64,
        "produce": pl.Int64,
        "total": pl.Int64,
    }
)


# Lakehouse loading contract

SNAPSHOT_PREFIX = (
    "snapshots/webdav-log-rescue/"
)

TABLE_NAME = "log_rescue"


def _integer_with_blank_as_zero(
    column: str,
) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=True)
        .str.strip_chars()
    )

    return value.replace("", "0").cast(
        pl.Int64,
        strict=True,
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        excel_compatible_date("Date").alias("rescue_date"),

        pl.col("Location")
        .cast(pl.String, strict=True)
        .alias("location"),

        _integer_with_blank_as_zero("Bakery")
        .alias("bakery"),

        _integer_with_blank_as_zero("Dairy")
        .alias("dairy"),

        _integer_with_blank_as_zero("Deli")
        .alias("deli"),

        _integer_with_blank_as_zero("Dry")
        .alias("dry"),

        _integer_with_blank_as_zero("Frozen")
        .alias("frozen"),

        _integer_with_blank_as_zero("Nonfood")
        .alias("nonfood"),

        _integer_with_blank_as_zero("Produce")
        .alias("produce"),

        _integer_with_blank_as_zero("Total")
        .alias("total"),
    )


def load(
    con,
    bucket: str,
    sql_string,
    require_nonempty_table,
) -> int:
    return load_latest_snapshot(
        con,
        bucket=bucket,
        snapshot_prefix=SNAPSHOT_PREFIX,
        table_name=TABLE_NAME,
        columns=OUTPUT_SCHEMA.names(),
        sql_string=sql_string,
        require_nonempty_table=require_nonempty_table,
    )


def sample(con) -> list[tuple]:
    return con.execute(
        f"""
        SELECT
            rescue_date,
            location,
            bakery,
            dairy,
            deli,
            dry,
            frozen,
            nonfood,
            produce,
            total
        FROM {TABLE_NAME}
        ORDER BY rescue_date, location
        LIMIT 5
        """
    ).fetchall()
