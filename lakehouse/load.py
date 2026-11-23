import os
import tempfile
from pathlib import Path

import boto3
import duckdb

from botocore.config import Config
from botocore.exceptions import ClientError


BUCKET = "data-bridge"
CATALOG_KEY = "lakehouse/catalog.duckdb"
DATA_PATH = "r2://data-bridge/lakehouse/data/"
OA_DISTRIBUTION_PREFIX = "snapshots/mealconnect-oadist/"


def r2_client():
    account_id = os.environ["CLOUDFLARE_R2_ACCOUNT_ID"]
    access_key = os.environ["CLOUDFLARE_R2_ACCESS_KEY_ID"]
    secret_key = os.environ["CLOUDFLARE_R2_SECRET_ACCESS_KEY"]

    return boto3.client(
        "s3",
        endpoint_url=(
            f"https://{account_id}.r2.cloudflarestorage.com"
        ),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def download_catalog(s3, catalog_path: Path) -> None:
    try:
        s3.download_file(
            BUCKET,
            CATALOG_KEY,
            str(catalog_path),
        )
    except ClientError as error:
        error_code = error.response["Error"].get("Code")

        if error_code not in {"404", "NoSuchKey"}:
            raise


def upload_catalog(s3, catalog_path: Path) -> None:
    s3.upload_file(
        str(catalog_path),
        BUCKET,
        CATALOG_KEY,
        ExtraArgs={
            "ContentType": "application/x-duckdb",
        },
    )


def list_snapshot_keys(s3, prefix: str) -> list[str]:
    keys = []
    continuation_token = None

    while True:
        request = {
            "Bucket": BUCKET,
            "Prefix": prefix,
        }

        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**request)

        for item in response.get("Contents", []):
            key = item["Key"]

            if key.endswith(".parquet"):
                keys.append(key)

        if not response.get("IsTruncated"):
            return sorted(keys)

        continuation_token = response["NextContinuationToken"]


def configure_r2(con: duckdb.DuckDBPyConnection) -> None:
    account_id = os.environ["CLOUDFLARE_R2_ACCOUNT_ID"]
    access_key = os.environ["CLOUDFLARE_R2_ACCESS_KEY_ID"]
    secret_key = os.environ["CLOUDFLARE_R2_SECRET_ACCESS_KEY"]

    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2_secret (
            TYPE R2,
            PROVIDER credential_chain,
            CHAIN 'env',
            ACCOUNT_ID '{account_id}'
        )
        """
    )


def load_oa_distribution(
    con: duckdb.DuckDBPyConnection,
    snapshot_keys: list[str],
) -> None:
    if not snapshot_keys:
        raise RuntimeError(
            "No OA Distribution snapshots were found"
        )

    snapshot_paths = [
        f"r2://{BUCKET}/{key}"
        for key in snapshot_keys
    ]

    con.read_parquet(
        snapshot_paths,
        filename=True,
    ).create_view("oa_distribution_snapshots")

    con.execute(
        """
        CREATE OR REPLACE TABLE lake.oa_distribution AS
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
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        distribution_date,
                        order_type,
                        order_location,
                        item_location,
                        item_name
                    ORDER BY filename DESC
                ) AS row_number
            FROM oa_distribution_snapshots
        )
        WHERE row_number = 1
        """
    )


def main() -> None:
    s3 = r2_client()

    with tempfile.TemporaryDirectory() as temporary_directory:
        catalog_path = (
            Path(temporary_directory)
            / "catalog.duckdb"
        )

        download_catalog(s3, catalog_path)

        con = duckdb.connect()

        try:
            configure_r2(con)

            con.execute(
                f"""
                ATTACH 'ducklake:{catalog_path}' AS lake
                (
                    DATA_PATH '{DATA_PATH}'
                )
                """
            )

            oa_distribution_keys = list_snapshot_keys(
                s3,
                OA_DISTRIBUTION_PREFIX,
            )

            load_oa_distribution(
                con,
                oa_distribution_keys,
            )

            con.execute("CHECKPOINT")

        finally:
            con.close()

        upload_catalog(s3, catalog_path)

    print("Lakehouse load successful")
    print("Loaded table: lake.oa_distribution")
    print(f"OA Distribution snapshots: {len(oa_distribution_keys)}")


if __name__ == "__main__":
    main()
