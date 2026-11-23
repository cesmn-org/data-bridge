CREATE TABLE log_warehouse_outgoing AS
SELECT
    DATE '1899-12-30' + CAST(Date AS INTEGER) AS transaction_date,
    CAST("Product Type" AS VARCHAR) AS product_type,
    SUM(CAST(NULLIF(TRIM(Pounds), '') AS INTEGER)) AS pounds
FROM input_outgoing
GROUP BY
    Date,
    "Product Type";
