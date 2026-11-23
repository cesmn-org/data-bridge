CREATE TABLE log_anonymous_programs AS
SELECT
    strptime(Date, '%m/%d/%Y')::DATE AS service_date,
    CAST(Program AS VARCHAR) AS program,
    COALESCE(CAST(NULLIF(TRIM(Households), '') AS INTEGER), 0)
        AS households,
    COALESCE(CAST(NULLIF(TRIM(Children), '') AS INTEGER), 0)
        AS children,
    COALESCE(CAST(NULLIF(TRIM(Adults), '') AS INTEGER), 0)
        AS adults,
    COALESCE(CAST(NULLIF(TRIM(Seniors), '') AS INTEGER), 0)
        AS seniors,
    COALESCE(CAST(NULLIF(TRIM(Pounds), '') AS INTEGER), 0)
        AS pounds,
    NULLIF(TRIM(Note), '')::VARCHAR AS note
FROM input_data;
