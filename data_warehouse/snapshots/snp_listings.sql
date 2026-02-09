{% snapshot snp_listings %}

{{
    config(
        target_database='airbnb_data_hub_gold',
        target_schema='airbnb_gold_snapshot',
        unique_key='listing_id',
        strategy='check',
        check_cols=[
            'host_id',
            'property_type',
            'room_type',
            'city',
            'country',
            'accommodates',
            'bedrooms',
            'bathrooms',
            'price_per_night'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
    listing_id,
    host_id,
    property_type,
    room_type,
    city,
    country,
    accommodates,
    bedrooms,
    bathrooms,
    price_per_night,
    created_at,
    _silver_loaded_at
FROM {{ ref('silver_listings') }}

{% endsnapshot %}