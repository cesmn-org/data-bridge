import polars as pl


RAW_COLUMNS = [
    "ID",
    "FirstName",
    "MiddleName",
    "LastName",
    "Suffix",
    "EmailAddress",
    "PhoneNumber",
    "AddressLine1",
    "AddressLine2",
    "Zipcode",
    "County",
    "City",
    "Province",
    "PostalCode",
    "DateOfBirth",
    "EmailContactApproved",
    "TextMessageContactApproved",
    "PhoneCallContactApproved",
    "HouseholdSize",
    "HouseholdAdultsCount",
    "HouseholdChildrenCount",
    "HouseholdSeniorsCount",
    "Gender",
    "Race",
    "HowDidYouHear",
    "IsParticipationSNAPNoted",
    "IsParticipationWICNoted",
    "IsParticipationMedicaidNoted",
    "IsParticipationSSINoted",
    "IsParticipationTANFNoted",
    "Created",
    "TotalOrderCount",
    "CancelledOrderCount",
    "PrimaryDeliveryAddressName",
    "PrimaryDeliveryAddressLine1",
    "PrimaryDeliveryAddressLine2",
    "PrimaryDeliveryAddressCity",
    "PrimaryDeliveryAddressState",
    "PrimaryDeliveryAddressZip",
    "PrimaryDeliveryInstructions",
    "IsFoodInsecurePerLatestResponse",
    "OrderID",
    "OrderNumber",
    "OrderStatus",
    "PlaceOfService",
    "DateOfService",
    "BirthDate",
    "Age",
    "NoFixedAddress",
    "OkToContact",
    "PreferredContactMethod",
    "GenderIdentity",
    "RaceEthnicity",
    "ProxyCount",
    "OtherGovtPrograms",
    "PreferredLanguage",
    "DisabilityStatus",
    "HouseholdMonthlyIncome",
    "LivingSituation",
    "EmploymentStatus",
    "MilitaryStatus",
    "FoodInsecurityWorried",
    "FoodInsecurityExperienced",
    "DietaryRestrictions",
    "FoodAllergies",
]


OUTPUT_SCHEMA = pl.Schema(
    {
        "client_id": pl.String,
        "email_contact_approved": pl.Boolean,
        "text_message_contact_approved": pl.Boolean,
        "phone_call_contact_approved": pl.Boolean,
        "household_size": pl.Int64,
        "household_adults_count": pl.Int64,
        "household_children_count": pl.Int64,
        "household_seniors_count": pl.Int64,
        "gender": pl.String,
        "race": pl.String,
        "how_did_you_hear": pl.String,
        "is_participation_snap_noted": pl.Boolean,
        "is_participation_wic_noted": pl.Boolean,
        "is_participation_medicaid_noted": pl.Boolean,
        "is_participation_ssi_noted": pl.Boolean,
        "is_participation_tanf_noted": pl.Boolean,
        "created": pl.Datetime(time_zone="UTC"),
        "total_order_count": pl.Int64,
        "cancelled_order_count": pl.Int64,
        "primary_delivery_address_city": pl.String,
        "primary_delivery_address_state": pl.String,
        "primary_delivery_address_zip": pl.String,
        "is_food_insecure_per_latest_response": pl.Boolean,
        "order_id": pl.String,
        "order_number": pl.String,
        "order_status": pl.String,
        "place_of_service": pl.String,
        "date_of_service": pl.Date,
        "birth_date": pl.Date,
        "age": pl.Int64,
        "no_fixed_address": pl.Boolean,
        "ok_to_contact": pl.Boolean,
        "preferred_contact_method": pl.String,
        "gender_identity": pl.String,
        "race_ethnicity": pl.String,
        "proxy_count": pl.Int64,
        "other_govt_programs": pl.String,
        "preferred_language": pl.String,
        "disability_status": pl.String,
        "household_monthly_income": pl.String,
        "living_situation": pl.String,
        "employment_status": pl.String,
        "military_status": pl.String,
        "food_insecurity_worried": pl.String,
        "food_insecurity_experienced": pl.String,
        "dietary_restrictions": pl.String,
        "food_allergies": pl.String,
    }
)


BOOLEAN_COLUMNS = [
    "EmailContactApproved",
    "TextMessageContactApproved",
    "PhoneCallContactApproved",
    "IsParticipationSNAPNoted",
    "IsParticipationWICNoted",
    "IsParticipationMedicaidNoted",
    "IsParticipationSSINoted",
    "IsParticipationTANFNoted",
    "IsFoodInsecurePerLatestResponse",
    "NoFixedAddress",
    "OkToContact",
]


# Lakehouse loading contract

SNAPSHOT_PREFIX = (
    "snapshots/mealconnect-oaorders/"
)

TABLE_NAME = "oa_orders"


def _nullable_string(column: str) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.strip_chars()
    )

    return (
        pl.when(value.is_null() | (value == ""))
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(value)
    )


def _nullable_integer(column: str) -> pl.Expr:
    return _nullable_string(column).cast(
        pl.Int64,
        strict=True,
    )


def _boolean(column: str) -> pl.Expr:
    value = (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .str.to_lowercase()
    )

    return (
        pl.when(value.is_null() | (value == ""))
        .then(pl.lit(None, dtype=pl.Boolean))
        .when(value == "true")
        .then(pl.lit(True))
        .when(value == "false")
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
    )


def _validate_boolean_values(df: pl.DataFrame) -> None:
    allowed = {"", "true", "false"}

    for column in BOOLEAN_COLUMNS:
        values = (
            df.get_column(column)
            .cast(pl.String, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_lowercase()
            .unique()
            .to_list()
        )

        invalid = sorted(
            value
            for value in values
            if value not in allowed
        )

        if invalid:
            raise ValueError(
                f"Invalid Boolean values in {column}: {invalid}"
            )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    _validate_boolean_values(df)

    return df.select(
        _nullable_string("ID").alias("client_id"),

        _boolean("EmailContactApproved")
        .alias("email_contact_approved"),

        _boolean("TextMessageContactApproved")
        .alias("text_message_contact_approved"),

        _boolean("PhoneCallContactApproved")
        .alias("phone_call_contact_approved"),

        _nullable_integer("HouseholdSize")
        .alias("household_size"),

        _nullable_integer("HouseholdAdultsCount")
        .alias("household_adults_count"),

        _nullable_integer("HouseholdChildrenCount")
        .alias("household_children_count"),

        _nullable_integer("HouseholdSeniorsCount")
        .alias("household_seniors_count"),

        _nullable_string("Gender").alias("gender"),

        _nullable_string("Race").alias("race"),

        _nullable_string("HowDidYouHear")
        .alias("how_did_you_hear"),

        _boolean("IsParticipationSNAPNoted")
        .alias("is_participation_snap_noted"),

        _boolean("IsParticipationWICNoted")
        .alias("is_participation_wic_noted"),

        _boolean("IsParticipationMedicaidNoted")
        .alias("is_participation_medicaid_noted"),

        _boolean("IsParticipationSSINoted")
        .alias("is_participation_ssi_noted"),

        _boolean("IsParticipationTANFNoted")
        .alias("is_participation_tanf_noted"),

        pl.col("Created")
        .str.strptime(
            pl.Datetime(time_zone="UTC"),
            format="%m/%d/%Y %H:%M:%S %z",
            strict=True,
        )
        .alias("created"),

        _nullable_integer("TotalOrderCount")
        .alias("total_order_count"),

        _nullable_integer("CancelledOrderCount")
        .alias("cancelled_order_count"),

        _nullable_string("PrimaryDeliveryAddressCity")
        .alias("primary_delivery_address_city"),

        _nullable_string("PrimaryDeliveryAddressState")
        .alias("primary_delivery_address_state"),

        _nullable_string("PrimaryDeliveryAddressZip")
        .alias("primary_delivery_address_zip"),

        _boolean("IsFoodInsecurePerLatestResponse")
        .alias("is_food_insecure_per_latest_response"),

        _nullable_string("OrderID").alias("order_id"),

        _nullable_string("OrderNumber")
        .alias("order_number"),

        _nullable_string("OrderStatus")
        .alias("order_status"),

        _nullable_string("PlaceOfService")
        .alias("place_of_service"),

        pl.col("DateOfService")
        .str.strptime(
            pl.Date,
            format="%Y-%m-%d",
            strict=True,
        )
        .alias("date_of_service"),

        pl.col("BirthDate")
        .str.strptime(
            pl.Date,
            format="%Y-%m-%d",
            strict=False,
        )
        .alias("birth_date"),

        _nullable_integer("Age")
        .alias("age"),

        _boolean("NoFixedAddress")
        .alias("no_fixed_address"),

        _boolean("OkToContact")
        .alias("ok_to_contact"),

        _nullable_string("PreferredContactMethod")
        .alias("preferred_contact_method"),

        _nullable_string("GenderIdentity")
        .alias("gender_identity"),

        _nullable_string("RaceEthnicity")
        .alias("race_ethnicity"),

        _nullable_integer("ProxyCount")
        .alias("proxy_count"),

        _nullable_string("OtherGovtPrograms")
        .alias("other_govt_programs"),

        _nullable_string("PreferredLanguage")
        .alias("preferred_language"),

        _nullable_string("DisabilityStatus")
        .alias("disability_status"),

        _nullable_string("HouseholdMonthlyIncome")
        .alias("household_monthly_income"),

        _nullable_string("LivingSituation")
        .alias("living_situation"),

        _nullable_string("EmploymentStatus")
        .alias("employment_status"),

        _nullable_string("MilitaryStatus")
        .alias("military_status"),

        _nullable_string("FoodInsecurityWorried")
        .alias("food_insecurity_worried"),

        _nullable_string("FoodInsecurityExperienced")
        .alias("food_insecurity_experienced"),

        _nullable_string("DietaryRestrictions")
        .alias("dietary_restrictions"),

        _nullable_string("FoodAllergies")
        .alias("food_allergies"),
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
            client_id,
            order_id,
            order_number,
            order_status,
            place_of_service,
            date_of_service,
            household_size,
            gender_identity,
            race_ethnicity
        FROM {TABLE_NAME}
        ORDER BY created, order_number
        LIMIT 5
        """
    ).fetchall()
