from datetime import date

import polars as pl


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


def _excel_date(column: str) -> pl.Expr:
    return (
        pl.lit(date(1899, 12, 30))
        + pl.duration(
            days=pl.col(column)
            .cast(pl.Float64, strict=True)
            .cast(pl.Int64, strict=True)
        )
    )


def _integer_with_blank_as_zero(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("", "0")
        .cast(pl.Int64, strict=True)
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        _excel_date("Date").alias("service_date"),

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
            .cast(pl.String, strict=False)
            .str.strip_chars()
            == ""
        )
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(
            pl.col("Note")
            .cast(pl.String, strict=False)
            .str.strip_chars()
        )
        .alias("note"),
    )
