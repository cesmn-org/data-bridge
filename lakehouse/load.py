import os


BUCKET = "data-bridge"
SNAPSHOT_PREFIX = "snapshots/"

OA_DISTRIBUTION_PREFIX = (
    "snapshots/mealconnect-oadist/"
)
ANONYMOUS_PROGRAMS_PREFIX = (
    "snapshots/webdav-log-anonymous-programs/"
)
RESCUE_PREFIX = (
    "snapshots/webdav-log-rescue/"
)

OA_DISTRIBUTION_TABLE = "oa_distribution"
ANONYMOUS_PROGRAMS_TABLE = "log_anonymous_programs"
RESCUE_TABLE = "log_rescue"

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

    snapshot_scope = f"r2://{BUCKET}/{SNAPSHOT_PREFIX}"

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
        f'USE "{database_name.replace(chr(34), chr(34) * 2)}"'
    )

    return database_name


def load_oa_distribution(con) -> int:
    snapshot_glob = (
        f"r2://{BUCKET}/"
        f"{OA_DISTRIBUTION_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {OA_DISTRIBUTION_TABLE} AS
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
        FROM {OA_DISTRIBUTION_TABLE}
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            "MotherDuck table is empty: "
            f"{OA_DISTRIBUTION_TABLE}"
        )

    return row_count


def load_anonymous_programs(con) -> int:
    snapshot_glob = (
        f"r2://{BUCKET}/"
        f"{ANONYMOUS_PROGRAMS_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {ANONYMOUS_PROGRAMS_TABLE} AS
        SELECT
            service_date,
            program,
            households,
            children,
            adults,
            seniors,
            pounds,
            note
        FROM (
            SELECT
                service_date,
                program,
                households,
                children,
                adults,
                seniors,
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

    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {ANONYMOUS_PROGRAMS_TABLE}
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            "MotherDuck table is empty: "
            f"{ANONYMOUS_PROGRAMS_TABLE}"
        )

    return row_count


def load_rescue(con) -> int:
    snapshot_glob = (
        f"r2://{BUCKET}/"
        f"{RESCUE_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {RESCUE_TABLE} AS
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
        FROM (
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
                total,
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

    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {RESCUE_TABLE}
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            "MotherDuck table is empty: "
            f"{RESCUE_TABLE}"
        )

    return row_count


def main() -> None:
    con = connect_to_motherduck()

    try:
        configure_r2_secret(con)
        database_name = select_ducklake_database(con)

        oa_distribution_row_count = (
            load_oa_distribution(con)
        )

        anonymous_programs_row_count = (
            load_anonymous_programs(con)
        )

        rescue_row_count = load_rescue(con)

        oa_distribution_sample = con.execute(
            f"""
            SELECT
                order_type,
                order_location,
                distribution_date,
                item_name,
                total_pounds,
                is_tefap
            FROM {OA_DISTRIBUTION_TABLE}
            ORDER BY distribution_date, item_name
            LIMIT 5
            """
        ).fetchall()

        anonymous_programs_sample = con.execute(
            f"""
            SELECT
                service_date,
                program,
                households,
                children,
                adults,
                seniors,
                pounds
            FROM {ANONYMOUS_PROGRAMS_TABLE}
            ORDER BY service_date, program
            LIMIT 5
            """
        ).fetchall()

        rescue_sample = con.execute(
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
            FROM {RESCUE_TABLE}
            ORDER BY rescue_date, location
            LIMIT 5
            """
        ).fetchall()

    finally:
        con.close()

    print("MotherDuck DuckLake load successful")
    print(f"Database: {database_name}")

    print(f"Table: {OA_DISTRIBUTION_TABLE}")
    print(f"Rows: {oa_distribution_row_count}")
    print("Sample rows:")

    for row in oa_distribution_sample:
        print(row)

    print(f"Table: {ANONYMOUS_PROGRAMS_TABLE}")
    print(f"Rows: {anonymous_programs_row_count}")
    print("Sample rows:")

    for row in anonymous_programs_sample:
        print(row)

    print(f"Table: {RESCUE_TABLE}")
    print(f"Rows: {rescue_row_count}")
    print("Sample rows:")

    for row in rescue_sample:
        print(row)


if __name__ == "__main__":
    main()
