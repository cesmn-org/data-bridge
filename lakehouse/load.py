import os


BUCKET = "data-bridge"
SNAPSHOT_PREFIX = "snapshots/mealconnect-oadist/"
TABLE_NAME = "oa_distribution"
R2_SECRET_NAME = "data_bridge_r2"


def required_environment(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable is missing: {name}"
        )

    return value


def sql_string(value: str) -> str:
    """Quote a value for use as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    """Quote a value for use as a SQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def connect_to_motherduck():
    import duckdb

    token = required_environment(
        "MOTHERDUCK_ACCESS_TOKEN"
    )

    return duckdb.connect(
        "md:",
        config={
            "motherduck_token": token,
        },
    )


def configure_r2_secret(con) -> None:
    account_id = required_environment(
        "CLOUDFLARE_R2_ACCOUNT_ID"
    )
    access_key = required_environment(
        "CLOUDFLARE_R2_ACCESS_KEY_ID"
    )
    secret_key = required_environment(
        "CLOUDFLARE_R2_SECRET_ACCESS_KEY"
    )

    snapshot_scope = (
        f"r2://{BUCKET}/{SNAPSHOT_PREFIX}"
    )

    con.execute(
        f"""
        CREATE OR REPLACE SECRET {R2_SECRET_NAME}
        IN MOTHERDUCK (
            TYPE R2,
            KEY_ID {sql_string(access_key)},
            SECRET {sql_string(secret_key)},
            ACCOUNT_ID {sql_string(account_id)},
            SCOPE {sql_string(snapshot_scope)}
        )
        """
    )


def select_ducklake_database(con) -> str:
    database_name = required_environment(
        "MOTHERDUCK_DUCKLAKE_NAME"
    )

    con.execute(
        f"USE {sql_identifier(database_name)}"
    )

    return database_name


def load_oa_distribution(con) -> int:
    snapshot_glob = (
        f"r2://{BUCKET}/{SNAPSHOT_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT
            order_type,
            order_location,
            distribution_date,
            item_location,
            item_name,
            quantity,
            unit_pounds,
            total_pounds,
            is_tefap
        FROM (
            SELECT
                order_type,
                order_location,
                distribution_date,
                item_location,
                item_name,
                quantity,
                unit_pounds,
                total_pounds,
                is_tefap,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        distribution_date,
                        order_type,
                        order_location,
                        item_location,
                        item_name
                    ORDER BY filename DESC
                ) AS row_number
            FROM read_parquet(
                {sql_string(snapshot_glob)},
                filename = true
            )
        )
        WHERE row_number = 1
        """
    )

    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE_NAME}
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            f"MotherDuck table is empty: {TABLE_NAME}"
        )

    return row_count


def main() -> None:
    con = connect_to_motherduck()

    try:
        configure_r2_secret(con)
        database_name = select_ducklake_database(con)

        row_count = load_oa_distribution(con)

        sample_rows = con.execute(
            f"""
            SELECT
                order_type,
                order_location,
                distribution_date,
                item_name,
                total_pounds,
                is_tefap
            FROM {TABLE_NAME}
            ORDER BY distribution_date, item_name
            LIMIT 5
            """
        ).fetchall()

    finally:
        con.close()

    print("MotherDuck DuckLake load successful")
    print(f"Database: {database_name}")
    print(f"Table: {TABLE_NAME}")
    print(f"Rows: {row_count}")
    print("Sample rows:")

    for row in sample_rows:
        print(row)


if __name__ == "__main__":
    main()
