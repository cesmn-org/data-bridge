CREATE TABLE oa_distribution AS
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
FROM input_data;
