{% macro update_control_watermark(wf_name) %}

    MERGE INTO {{ source('gold', 'control_table') }} AS tgt
    USING (
        SELECT
            '{{ wf_name }}'          AS wf_name,
            current_timestamp()      AS last_processed_at
    ) AS src
    ON tgt.wf_name = src.wf_name
    WHEN MATCHED THEN
        UPDATE SET
            tgt.last_processed_at = src.last_processed_at
    WHEN NOT MATCHED THEN
        INSERT (wf_name, last_processed_at)
        VALUES (src.wf_name, src.last_processed_at)
    ;

{% endmacro %}