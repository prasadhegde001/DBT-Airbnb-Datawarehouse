{{
      config(
        materialized = 'incremental',
        catalog = 'airbnb_data_hub_silver',
        schema = 'airbnb_silver_db',
        file_format = 'delta',
        partition_by = ["year", "month", "day"],
        incremental_strategy='merge',
        unique_key='booking_id',
        merge_update_columns=[
            'nights_booked',
            'booking_amount',
            'cleaning_fee',
            'service_fee',
            'booking_status',
            '_silver_loaded_at'
        ],
        tags=['silver']
        )
}}

with source_data as (
    select 
            booking_id,
            listing_id,
            booking_date,
            nights_booked,
            booking_amount,
            cleaning_fee,
            service_fee,
            booking_status,
            created_at,
            _bronze_loaded_at,
             year,
             month,
             day
            from {{ ref('bronze_bookings_v1') }}
            if {% if is_incremental() %}
                where _bronze_loaded_at > ( select coalesce(max(_silver_loaded_at), '1900-01-01') from {{ this }} )
            {% endif %}

),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY _bronze_loaded_at DESC
        ) AS _rn
    FROM source_data
)
SELECT
    *,
    current_timestamp() AS _silver_loaded_at
FROM deduplicated
WHERE _rn = 1
