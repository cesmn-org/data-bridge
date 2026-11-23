"""Shared Polars helpers for source fetch contracts."""

from datetime import date

import polars as pl


def excel_compatible_date(column: str) -> pl.Expr:
    """Parse ISO dates, ISO datetimes, or Excel serial dates."""
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
        + pl.duration(days=value.cast(pl.Int64, strict=False))
    ).cast(pl.Date)

    return pl.coalesce(
        [
            parsed_datetime,
            parsed_date,
            excel_serial,
        ]
    )
