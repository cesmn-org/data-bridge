CREATE TABLE log_warehouse_outgoing AS
SELECT
    DATE '1899-12-30' + CAST(Date AS INTEGER) AS transaction_date,
    CAST("Product Type" AS VARCHAR) AS product_type,
    CAST(NULLIF(TRIM(Pounds), '') AS INTEGER) AS pounds,
    NULLIF(TRIM(Note), '')::VARCHAR AS note
FROM input_outgoing;
