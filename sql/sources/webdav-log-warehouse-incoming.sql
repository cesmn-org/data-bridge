CREATE TABLE log_warehouse_incoming AS
SELECT
    DATE '1899-12-30' + CAST(Date AS INTEGER) AS transaction_date,
    CAST(Source AS VARCHAR) AS source,
    CAST("Transaction Type" AS VARCHAR) AS transaction_type,
    CAST("Product Type" AS VARCHAR) AS product_type,
    CAST(NULLIF(TRIM(Pounds), '') AS INTEGER) AS pounds,
    NULLIF(TRIM(Note), '')::VARCHAR AS note
FROM input_incoming;
