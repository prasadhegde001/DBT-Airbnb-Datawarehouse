{% macro get_control_watermark(wf_name) %}
    (
        SELECT
            COALESCE(
                MAX(last_processed_at),
                CAST('2000-01-01 00:00:00' AS TIMESTAMP)
            )
        FROM {{ source('gold', 'control_table') }}
        WHERE wf_name = '{{ wf_name }}'
    )
{% endmacro %}