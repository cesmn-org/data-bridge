import os
from importlib.util import module_from_spec
from importlib.util import spec_from_file_location
from pathlib import Path


BUCKET = "data-bridge"
SNAPSHOT_PREFIX = "snapshots/"
R2_SECRET_NAME = "data_bridge_r2"

CONTRACTS_DIRECTORY = (
    Path(__file__).resolve().parent.parent
    / "contracts"
)


def required_environment(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable is missing: {name}"
        )

    return value


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_contract(filename: str):
    contract_path = CONTRACTS_DIRECTORY / filename

    module_name = (
        "data_bridge_contract_"
        + filename.replace("-", "_").replace(".", "_")
    )

    spec = spec_from_file_location(
        module_name,
        contract_path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Could not load contract: {contract_path}"
        )

    contract = module_from_spec(spec)
    spec.loader.exec_module(contract)

    return contract


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

    escaped_name = database_name.replace(
        '"',
        '""',
    )

    con.execute(f'USE "{escaped_name}"')

    return database_name


def require_nonempty_table(
    con,
    table_name: str,
) -> int:
    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{table_name.replace('"', '""')}"
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            f"MotherDuck table is empty: {table_name}"
        )

    return row_count


def load_contract_table(
    contract,
    con,
) -> int:
    return contract.load(
        con=con,
        bucket=BUCKET,
        sql_string=sql_string,
        require_nonempty_table=require_nonempty_table,
    )


def print_sample(
    table_name: str,
    row_count: int,
    rows: list[tuple],
) -> None:
    print(
        f"Table: {table_name}; "
        f"rows: {row_count}"
    )
    print("Sample rows:")

    for row in rows:
        print(row)


def main() -> None:
    mealconnect_oadist = load_contract(
        "mealconnect-oadist.py"
    )

    webdav_anonymous_programs = load_contract(
        "webdav-log-anonymous-programs.py"
    )

    webdav_rescue = load_contract(
        "webdav-log-rescue.py"
    )

    webdav_warehouse = load_contract(
        "webdav-log-warehouse.py"
    )

    mealconnect_oaorders = load_contract(
        "mealconnect-oaorders.py"
    )

    clickncollect_orders = load_contract(
        "clickncollect-orders.py"
    )

    con = connect_to_motherduck()

    try:
        configure_r2_secret(con)
        database_name = select_ducklake_database(con)

        oa_distribution_row_count = (
            load_contract_table(
                mealconnect_oadist,
                con,
            )
        )

        anonymous_programs_row_count = (
            load_contract_table(
                webdav_anonymous_programs,
                con,
            )
        )

        rescue_row_count = load_contract_table(
            webdav_rescue,
            con,
        )

        warehouse_incoming_row_count = (
            webdav_warehouse.load_incoming(
                con=con,
                bucket=BUCKET,
                sql_string=sql_string,
                require_nonempty_table=(
                    require_nonempty_table
                ),
            )
        )

        warehouse_outgoing_row_count = (
            webdav_warehouse.load_outgoing(
                con=con,
                bucket=BUCKET,
                sql_string=sql_string,
                require_nonempty_table=(
                    require_nonempty_table
                ),
            )
        )

        oa_orders_row_count = load_contract_table(
            mealconnect_oaorders,
            con,
        )

        cnc_orders_row_count = load_contract_table(
            clickncollect_orders,
            con,
        )

        oa_distribution_sample = (
            mealconnect_oadist.sample(con)
        )

        anonymous_programs_sample = (
            webdav_anonymous_programs.sample(con)
        )

        rescue_sample = webdav_rescue.sample(con)

        warehouse_incoming_sample = (
            webdav_warehouse.sample_incoming(con)
        )

        warehouse_outgoing_sample = (
            webdav_warehouse.sample_outgoing(con)
        )

        oa_orders_sample = (
            mealconnect_oaorders.sample(con)
        )

        cnc_orders_sample = (
            clickncollect_orders.sample(con)
        )

    finally:
        con.close()

    print("MotherDuck DuckLake load successful")
    print(f"Database: {database_name}")

    print_sample(
        mealconnect_oadist.TABLE_NAME,
        oa_distribution_row_count,
        oa_distribution_sample,
    )

    print_sample(
        webdav_anonymous_programs.TABLE_NAME,
        anonymous_programs_row_count,
        anonymous_programs_sample,
    )

    print_sample(
        webdav_rescue.TABLE_NAME,
        rescue_row_count,
        rescue_sample,
    )

    print_sample(
        webdav_warehouse.INCOMING_TABLE_NAME,
        warehouse_incoming_row_count,
        warehouse_incoming_sample,
    )

    print_sample(
        webdav_warehouse.OUTGOING_TABLE_NAME,
        warehouse_outgoing_row_count,
        warehouse_outgoing_sample,
    )

    print_sample(
        mealconnect_oaorders.TABLE_NAME,
        oa_orders_row_count,
        oa_orders_sample,
    )

    print_sample(
        clickncollect_orders.TABLE_NAME,
        cnc_orders_row_count,
        cnc_orders_sample,
    )


if __name__ == "__main__":
    main()
