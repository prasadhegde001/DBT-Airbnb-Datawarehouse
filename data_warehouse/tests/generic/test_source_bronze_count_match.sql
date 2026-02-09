{% test source_bronze_count_match(model, source_name, source_table) %}

    WITH source_count AS (
        SELECT COUNT(*) AS cnt
        FROM {{ source(source_name, source_table) }}
    ),

    bronze_count AS (
        SELECT COUNT(*) AS cnt
        FROM {{ model }}
    )

    -- Test FAILS if this query returns any rows
    SELECT
        s.cnt AS source_count,
        b.cnt AS bronze_count,
        s.cnt - b.cnt AS difference
    FROM source_count s
    CROSS JOIN bronze_count b
    WHERE s.cnt != b.cnt

{% endtest %}
