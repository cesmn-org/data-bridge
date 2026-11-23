import polars as pl


RAW_COLUMNS = [
    "Location",
    "Locker Terminal",
    "Order ID",
    "Tracking ID",
    "Locker Number",
    "First Name",
    "Last Name",
    "Mobile",
    "Email",
    "Address 1",
    "Address 2",
    "City",
    "Country",
    "Post Code",
    "Date",
    "Locker Status",
    "Courier",
    "Delivery Agency",
    "Delivery Date",
    "Pick Up Date",
]


OUTPUT_SCHEMA = pl.Schema(
    {
        "location": pl.String,
        "locker_terminal": pl.String,
        "order_id": pl.String,
        "tracking_id": pl.String,
        "locker_number": pl.String,
        "city": pl.String,
        "country": pl.String,
        "post_code": pl.String,
        "order_date": pl.Date,
        "locker_status": pl.String,
        "courier": pl.String,
        "delivery_agency": pl.String,
        "delivery_datetime": pl.Datetime(
            time_zone="America/Chicago"
        ),
        "pickup_datetime": pl.Datetime(
            time_zone="America/Chicago"
        ),
    }
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


def _nullable_datetime(column: str) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=True)
        .str.strip_chars()
    )

    return (
        value
        .str.strptime(
            pl.Datetime,
            format="%d/%m/%Y %H:%M",
            strict=False,
        )
        .dt.replace_time_zone("America/Chicago")
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        _nullable_string("Location")
        .alias("location"),

        _nullable_string("Locker Terminal")
        .alias("locker_terminal"),

        _nullable_string("Order ID")
        .alias("order_id"),

        _nullable_string("Tracking ID")
        .alias("tracking_id"),

        _nullable_string("Locker Number")
        .alias("locker_number"),

        _nullable_string("City")
        .alias("city"),

        _nullable_string("Country")
        .alias("country"),

        _nullable_string("Post Code")
        .alias("post_code"),

        pl.col("Date")
        .cast(pl.String, strict=True)
        .str.strip_chars()
        .str.strptime(
            pl.Date,
            format="%d/%m/%Y",
            strict=True,
        )
        .alias("order_date"),

        _nullable_string("Locker Status")
        .alias("locker_status"),

        _nullable_string("Courier")
        .alias("courier"),

        _nullable_string("Delivery Agency")
        .alias("delivery_agency"),

        _nullable_datetime("Delivery Date")
        .alias("delivery_datetime"),

        _nullable_datetime("Pick Up Date")
        .alias("pickup_datetime"),
    )
