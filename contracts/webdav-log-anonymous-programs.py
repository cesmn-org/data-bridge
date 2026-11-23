from datetime import date

import polars as pl

from lakehouse.common import load_latest_snapshot


RAW_COLUMNS = [
    "Date",
    "Program",
    "Households",
    "Children",
    "Adults",
    "Seniors",
    "Pounds",
    "Note",
]


OUTPUT_SCHEMA = pl.Schema(
    {
        "service_date": pl.Date,
        "program": pl.String,
        "households": pl.Int64,
        "children": pl.Int64,
        "adults": pl.Int64,
        "seniors": pl.Int64,
        "pounds": pl.Int64,
        "note": pl.String,
    }
)


# Lakehouse loading contract

SNAPSHOT_PREFIX = (
    "snapshots/webdav-log-anonymous-programs/"
)

TABLE_NAME = "log_anonymous_programs"


def _service_date() -> pl.Expr:
    value = (
        pl.col("Date")
        .cast(pl.String, strict=True)
        .str.strip_chars()
    )

    parsed_datetime = value.str.strptime(
        pl.Datetime,
        format="%Y-%m-%d %H:%M:%S",
        strict=False,
    ).dt.date()

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


def _integer_with_blank_as_zero(
    column: str,
) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=True)
        .str.strip_chars()
    )

    return (
        value
        .replace("", "0")
        .cast(pl.Int64, strict=True)
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        _service_date().alias("service_date"),

        pl.col("Program")
        .cast(pl.String, strict=True)
        .alias("program"),

        _integer_with_blank_as_zero("Households")
        .alias("households"),

        _integer_with_blank_as_zero("Children")
        .alias("children"),

        _integer_with_blank_as_zero("Adults")
        .alias("adults"),

        _integer_with_blank_as_zero("Seniors")
        .alias("seniors"),

        _integer_with_blank_as_zero("Pounds")
        .alias("pounds"),

        pl.when(
            pl.col("Note")
            .cast(pl.String, strict=True)
            .str.strip_chars()
            == ""
        )
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(
            pl.col("Note")
            .cast(pl.String, strict=True)
            .str.strip_chars()
        )
        .alias("note"),
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
            service_date,
            program,
            households,
            children,
            adults,
            seniors,
            pounds
        FROM {TABLE_NAME}
        ORDER BY service_date, program
        LIMIT 5
        """
    ).fetchall()
