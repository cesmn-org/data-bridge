CREATE TABLE log_warehouse_incoming AS
SELECT
    DATE '1899-12-30' + CAST(Date AS INTEGER) AS transaction_date,
    CAST(Source AS VARCHAR) AS source,
    CAST("Transaction Type" AS VARCHAR) AS transaction_type,
    CAST("Product Type" AS VARCHAR) AS product_type,
    SUM(CAST(NULLIF(TRIM(Pounds), '') AS INTEGER)) AS pounds
FROM input_incoming
GROUP BY
    Date,
    Source,
    "Transaction Type",
    "Product Type";
