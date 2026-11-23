CREATE TEMP TABLE raw_oadist (
    OrderType VARCHAR,
    OrderLocation VARCHAR,
    Date VARCHAR,
    ItemLocation VARCHAR,
    ItemName VARCHAR,
    Quantity VARCHAR,
    UnitPounds VARCHAR,
    TotalPounds VARCHAR,
    IsTEFAP VARCHAR
);

CREATE TEMP TABLE actual_input_schema AS
SELECT
    ROW_NUMBER() OVER () AS column_position,
    column_name
FROM (
    DESCRIBE input_data
);

CREATE TEMP TABLE expected_input_schema AS
SELECT
    ROW_NUMBER() OVER () AS column_position,
    column_name
FROM (
    DESCRIBE raw_oadist
);

WITH schema_comparison AS (
    SELECT
        actual.column_position,
        actual.column_name AS actual_column_name,
        expected.column_name AS expected_column_name
    FROM actual_input_schema AS actual
    FULL OUTER JOIN expected_input_schema AS expected
        ON actual.column_position = expected.column_position
)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM schema_comparison
        WHERE actual_column_name IS DISTINCT FROM expected_column_name
    )
    THEN 1
    ELSE error(
        'OA Distribution input schema does not match the SQL contract'
    )
END;

INSERT INTO raw_oadist BY NAME
SELECT *
FROM input_data;

CREATE TABLE oa_distribution (
    order_type VARCHAR,
    order_location VARCHAR,
    distribution_date DATE,
    item_location VARCHAR,
    item_name VARCHAR,
    quantity INTEGER,
    unit_pounds DECIMAL(12, 2),
    total_pounds DECIMAL(12, 2),
    is_tefap BOOLEAN
);

INSERT INTO oa_distribution
SELECT
    CAST(OrderType AS VARCHAR) AS order_type,
    CAST(OrderLocation AS VARCHAR) AS order_location,
    strptime(Date, '%m-%d-%Y')::DATE AS distribution_date,
    NULLIF(TRIM(ItemLocation), '')::VARCHAR AS item_location,
    CAST(ItemName AS VARCHAR) AS item_name,
    CAST(Quantity AS INTEGER) AS quantity,
    CAST(UnitPounds AS DECIMAL(12, 2)) AS unit_pounds,
    CAST(TotalPounds AS DECIMAL(12, 2)) AS total_pounds,
    CAST(IsTEFAP AS BOOLEAN) AS is_tefap
FROM raw_oadist;
