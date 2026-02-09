{{
    config(
        materialized='table',
        catalog = 'airbnb_data_hub_gold',
        schema = 'airbnb_gold_db',
        file_format='delta',
        tags=['gold', 'dimension']
    )
}}

WITH date_spine AS (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast('2030-12-31' as date)"
        )
    }}
)

SELECT
    -- Key
    CAST(DATE_FORMAT(date_day, 'yyyyMMdd') AS INT)      AS date_key,

    -- Attributes
    date_day                                             AS full_date,
    DAYOFWEEK(date_day)                                  AS day_of_week,
    DATE_FORMAT(date_day, 'EEEE')                        AS day_name,
    DAY(date_day)                                        AS day_of_month,
    MONTH(date_day)                                      AS month_number,
    DATE_FORMAT(date_day, 'MMMM')                        AS month_name,
    QUARTER(date_day)                                    AS quarter,
    YEAR(date_day)                                       AS year,
    WEEKOFYEAR(date_day)                                 AS week_of_year,

    -- Flags
    CASE
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                                  AS is_weekend,

    -- Fiscal year (April start — adjust as needed)
    CASE
        WHEN MONTH(date_day) >= 4 THEN YEAR(date_day)
        ELSE YEAR(date_day) - 1
    END                                                  AS fiscal_year

FROM date_spine
