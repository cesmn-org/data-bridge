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


# Lakehouse loading contract

SNAPSHOT_PREFIX = (
    "snapshots/clickncollect-orders/"
)

TABLE_NAME = "click_n_collect_orders"


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


def load(
    con,
    bucket: str,
    sql_string,
    require_nonempty_table,
) -> int:
    snapshot_glob = (
        f"r2://{bucket}/"
        f"{SNAPSHOT_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT * EXCLUDE (filename)
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

    return require_nonempty_table(
        con,
        TABLE_NAME,
    )


def sample(con) -> list[tuple]:
    return con.execute(
        f"""
        SELECT
            location,
            locker_terminal,
            order_id,
            tracking_id,
            locker_number,
            order_date,
            locker_status,
            delivery_datetime,
            pickup_datetime
        FROM {TABLE_NAME}
        ORDER BY order_date, tracking_id
        LIMIT 5
        """
    ).fetchall()
