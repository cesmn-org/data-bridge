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
WAREHOUSE_INCOMING_PREFIX = (
    "snapshots/webdav-log-warehouse-incoming/"
)
WAREHOUSE_OUTGOING_PREFIX = (
    "snapshots/webdav-log-warehouse-outgoing/"
)

OA_DISTRIBUTION_TABLE = "oa_distribution"
ANONYMOUS_PROGRAMS_TABLE = "log_anonymous_programs"
RESCUE_TABLE = "log_rescue"
WAREHOUSE_INCOMING_TABLE = "log_warehouse_incoming"
WAREHOUSE_OUTGOING_TABLE = "log_warehouse_outgoing"

R2_SECRET_NAME = "data_bridge_r2"


def required_environment(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable is missing: {name}"
        )

    return value


def sql_string(value: str) -> str:
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

    escaped_name = database_name.replace('"', '""')

    con.execute(f'USE "{escaped_name}"')

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

    return require_nonempty_table(
        con,
        OA_DISTRIBUTION_TABLE,
    )


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

    return require_nonempty_table(
        con,
        ANONYMOUS_PROGRAMS_TABLE,
    )


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

    return require_nonempty_table(
        con,
        RESCUE_TABLE,
    )


def load_warehouse_incoming(con) -> int:
    snapshot_glob = (
        f"r2://{BUCKET}/"
        f"{WAREHOUSE_INCOMING_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {WAREHOUSE_INCOMING_TABLE} AS
        SELECT
            transaction_date,
            source,
            transaction_type,
            product_type,
            pounds,
            note
        FROM (
            SELECT
                transaction_date,
                source,
                transaction_type,
                product_type,
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

    return require_nonempty_table(
        con,
        WAREHOUSE_INCOMING_TABLE,
    )


def load_warehouse_outgoing(con) -> int:
    snapshot_glob = (
        f"r2://{BUCKET}/"
        f"{WAREHOUSE_OUTGOING_PREFIX}*.parquet"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {WAREHOUSE_OUTGOING_TABLE} AS
        SELECT
            transaction_date,
            product_type,
            pounds,
            note
        FROM (
            SELECT
                transaction_date,
                product_type,
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

    return require_nonempty_table(
        con,
        WAREHOUSE_OUTGOING_TABLE,
    )


def require_nonempty_table(con, table_name: str) -> int:
    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            f"MotherDuck table is empty: {table_name}"
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
        warehouse_incoming_row_count = (
            load_warehouse_incoming(con)
        )
        warehouse_outgoing_row_count = (
            load_warehouse_outgoing(con)
        )

        warehouse_incoming_sample = con.execute(
            f"""
            SELECT
                transaction_date,
                source,
                transaction_type,
                product_type,
                pounds,
                note
            FROM {WAREHOUSE_INCOMING_TABLE}
            ORDER BY transaction_date, source
            LIMIT 5
            """
        ).fetchall()

        warehouse_outgoing_sample = con.execute(
            f"""
            SELECT
                transaction_date,
                product_type,
                pounds,
                note
            FROM {WAREHOUSE_OUTGOING_TABLE}
            ORDER BY transaction_date, product_type
            LIMIT 5
            """
        ).fetchall()

    finally:
        con.close()

    print("MotherDuck DuckLake load successful")
    print(f"Database: {database_name}")

    print(
        f"Table: {OA_DISTRIBUTION_TABLE}; "
        f"rows: {oa_distribution_row_count}"
    )
    print(
        f"Table: {ANONYMOUS_PROGRAMS_TABLE}; "
        f"rows: {anonymous_programs_row_count}"
    )
    print(
        f"Table: {RESCUE_TABLE}; "
        f"rows: {rescue_row_count}"
    )
    print(
        f"Table: {WAREHOUSE_INCOMING_TABLE}; "
        f"rows: {warehouse_incoming_row_count}"
    )
    print("Sample incoming rows:")

    for row in warehouse_incoming_sample:
        print(row)

    print(
        f"Table: {WAREHOUSE_OUTGOING_TABLE}; "
        f"rows: {warehouse_outgoing_row_count}"
    )
    print("Sample outgoing rows:")

    for row in warehouse_outgoing_sample:
        print(row)


if __name__ == "__main__":
    main()
