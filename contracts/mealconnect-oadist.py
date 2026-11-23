import polars as pl


RAW_COLUMNS = [
    "OrderType",
    "OrderLocation",
    "Date",
    "ItemLocation",
    "ItemName",
    "Quantity",
    "UnitPounds",
    "TotalPounds",
    "IsTEFAP",
]


OUTPUT_SCHEMA = pl.Schema(
    {
        "order_type": pl.String,
        "order_location": pl.String,
        "distribution_date": pl.Date,
        "item_location": pl.String,
        "item_name": pl.String,
        "quantity": pl.Int64,
        "unit_pounds": pl.Decimal(precision=12, scale=2),
        "total_pounds": pl.Decimal(precision=12, scale=2),
        "is_tefap": pl.Boolean,
    }
)


def transform(df: pl.DataFrame) -> pl.DataFrame:
    tefap_values = (
        df.get_column("IsTEFAP")
        .str.strip_chars()
        .str.to_lowercase()
        .unique()
        .to_list()
    )

    invalid_tefap_values = sorted(
        value
        for value in tefap_values
        if value not in {"true", "false"}
    )

    if invalid_tefap_values:
        raise ValueError(
            "Invalid IsTEFAP values: "
            f"{invalid_tefap_values}"
        )

    return df.select(
        pl.col("OrderType")
        .cast(pl.String, strict=True)
        .alias("order_type"),

        pl.col("OrderLocation")
        .cast(pl.String, strict=True)
        .alias("order_location"),

        pl.col("Date")
        .str.strptime(
            pl.Date,
            format="%m-%d-%Y",
            strict=True,
        )
        .alias("distribution_date"),

        pl.when(
            pl.col("ItemLocation").str.strip_chars() == ""
        )
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(
            pl.col("ItemLocation").str.strip_chars()
        )
        .cast(pl.String, strict=True)
        .alias("item_location"),

        pl.col("ItemName")
        .cast(pl.String, strict=True)
        .alias("item_name"),

        pl.col("Quantity")
        .cast(pl.Int64, strict=True)
        .alias("quantity"),

        pl.col("UnitPounds")
        .cast(
            OUTPUT_SCHEMA["unit_pounds"],
            strict=True,
        )
        .alias("unit_pounds"),

        pl.col("TotalPounds")
        .cast(
            OUTPUT_SCHEMA["total_pounds"],
            strict=True,
        )
        .alias("total_pounds"),

        pl.when(
            pl.col("IsTEFAP")
            .str.strip_chars()
            .str.to_lowercase()
            == "true"
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("is_tefap"),
    )
