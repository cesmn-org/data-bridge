import polars as pl

from contracts.common import nullable_string
from lakehouse.common import load_latest_snapshot


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
        nullable_string("Location")
        .alias("location"),

        nullable_string("Locker Terminal")
        .alias("locker_terminal"),

        nullable_string("Order ID")
        .alias("order_id"),

        nullable_string("Tracking ID")
        .alias("tracking_id"),

        nullable_string("Locker Number")
        .alias("locker_number"),

        nullable_string("City")
        .alias("city"),

        nullable_string("Country")
        .alias("country"),

        nullable_string("Post Code")
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

        nullable_string("Locker Status")
        .alias("locker_status"),

        nullable_string("Courier")
        .alias("courier"),

        nullable_string("Delivery Agency")
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
