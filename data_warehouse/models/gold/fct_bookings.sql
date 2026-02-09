{{
    config(
        materialized='incremental',
        catalog = 'airbnb_data_hub_gold',
        schema = 'airbnb_gold_db',
        unique_key='booking_id',
        incremental_strategy='merge',
        file_format='delta',
        partition_by=['booking_date_key'],
        merge_update_columns=[
            'listing_key',
            'host_key',
            'booking_status_key',
            'nights_booked',
            'booking_amount',
            'cleaning_fee',
            'service_fee',
            'total_revenue',
            'avg_amount_per_night',
            '_gold_loaded_at'
        ],
        post_hook="{{ update_control_watermark('fct_bookings') }}",
        tags=['gold', 'fact']
    )
}}

WITH new_bookings AS (
    SELECT
        booking_id,
        listing_id,
        booking_date,
        nights_booked,
        booking_amount,
        cleaning_fee,
        service_fee,
        booking_status,
        _silver_loaded_at
    FROM {{ ref('silver_bookings') }}
    WHERE _silver_loaded_at > {{ get_control_watermark('fct_bookings') }}
),

fact_with_keys AS (
    SELECT
        b.booking_id,
        CAST(DATE_FORMAT(b.booking_date, 'yyyyMMdd') AS INT)    AS booking_date_key,
        dl.listing_key,
        dh.host_key,
        COALESCE(ds.booking_status_key, -1)                     AS booking_status_key,
        b.nights_booked,
        b.booking_amount,
        b.cleaning_fee,
        b.service_fee,
        (b.booking_amount + b.cleaning_fee + b.service_fee)     AS total_revenue,
        ROUND(
            b.booking_amount / NULLIF(b.nights_booked, 0), 2
        )                                                        AS avg_amount_per_night,
        current_timestamp()                                      AS _gold_loaded_at

    FROM new_bookings b

    LEFT JOIN {{ ref('dim_listing') }} dl
        ON  b.listing_id = dl.listing_id
        AND b.booking_date >= dl.effective_from
        AND (b.booking_date < dl.effective_to OR dl.effective_to IS NULL)

    LEFT JOIN {{ ref('dim_listing') }} dl2
        ON  b.listing_id = dl2.listing_id
        AND dl2.is_current = TRUE

    LEFT JOIN {{ ref('dim_host') }} dh
        ON  dl2.host_id = dh.host_id
        AND b.booking_date >= dh.effective_from
        AND (b.booking_date < dh.effective_to OR dh.effective_to IS NULL)

    LEFT JOIN {{ ref('dim_booking_status') }} ds
        ON b.booking_status = ds.booking_status
)

SELECT * FROM fact_with_keys