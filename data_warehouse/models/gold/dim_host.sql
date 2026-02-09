{{
    config(
        materialized='incremental',
        catalog = 'airbnb_data_hub_gold',
        schema = 'airbnb_gold_db',
        unique_key='host_key',
        incremental_strategy='merge',
        file_format='delta',
        merge_update_columns=[
            'host_name',
            'host_since',
            'is_superhost',
            'response_rate',
            'effective_from',
            'effective_to',
            'is_current',
            'dbt_scd_id',
            'dbt_updated_at'
        ],
        post_hook="{{ update_control_watermark('dim_host') }}",
        tags=['gold', 'dimension']
    )
}}

WITH snapshot_data AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'host_id',
            'dbt_valid_from'
        ]) }}                                           AS host_key,

        -- Natural Key
        host_id,

        -- Attributes
        host_name,
        host_since,
        is_superhost,
        response_rate,

        -- SCD Type 2 Tracking
        dbt_valid_from                                  AS effective_from,
        dbt_valid_to                                    AS effective_to,
        CASE
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END                                             AS is_current,

        -- Metadata
        dbt_scd_id,
        dbt_updated_at

    FROM {{ ref('snp_hosts') }}

    -- Only pick up new/changed records from snapshot
    WHERE dbt_updated_at > {{ get_control_watermark('dim_host') }}
)

SELECT * FROM snapshot_data