CREATE TABLE log_rescue AS
SELECT
    DATE '1899-12-30' + CAST(Date AS INTEGER) AS rescue_date,
    CAST(Location AS VARCHAR) AS location,
    COALESCE(
        CAST(NULLIF(TRIM(Bakery), '') AS INTEGER),
        0
    ) AS bakery,
    COALESCE(
        CAST(NULLIF(TRIM(Dairy), '') AS INTEGER),
        0
    ) AS dairy,
    COALESCE(
        CAST(NULLIF(TRIM(Deli), '') AS INTEGER),
        0
    ) AS deli,
    COALESCE(
        CAST(NULLIF(TRIM(Dry), '') AS INTEGER),
        0
    ) AS dry,
    COALESCE(
        CAST(NULLIF(TRIM(Frozen), '') AS INTEGER),
        0
    ) AS frozen,
    COALESCE(
        CAST(NULLIF(TRIM(Nonfood), '') AS INTEGER),
        0
    ) AS nonfood,
    COALESCE(
        CAST(NULLIF(TRIM(Produce), '') AS INTEGER),
        0
    ) AS produce,
    COALESCE(
        CAST(NULLIF(TRIM(Total), '') AS INTEGER),
        0
    ) AS total
FROM input_data;
