{{
      config(
        materialized = 'incremental',
        catalog = 'airbnb_data_hub_silver',
        schema = 'airbnb_silver_db',
        file_format = 'delta',
        partition_by = ["year", "month", "day"],
        incremental_strategy='merge',
        unique_key='host_id',
        merge_update_columns=[
            'host_name',
            'host_since',
            'is_superhost',
            'response_rate',
            '_silver_loaded_at'
        ],
        tags=['silver'] 
        )
}}

with source_data as (
        select 
            host_id,
            host_name,
            host_since,
            is_superhost,
            response_rate,
            created_at,
            _bronze_loaded_at,
             year,
             month,
             day
            from {{ ref('bronze_hosts_v1') }}
            if {% if is_incremental() %}
                where _bronze_loaded_at > ( select coalesce(max(_silver_loaded_at), '1900-01-01') from {{ this }} )
            {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY host_id
            ORDER BY _bronze_loaded_at DESC
        ) AS _rn
    FROM source_data
)
SELECT
    *,
    current_timestamp() AS _silver_loaded_at
FROM deduplicated
WHERE _rn = 1