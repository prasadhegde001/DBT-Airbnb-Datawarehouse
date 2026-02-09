{{
    config(
        materialized='incremental',
        catalog = 'airbnb_data_hub_gold',
        schema = 'airbnb_gold_db',
        unique_key='listing_key',
        incremental_strategy='merge',
        file_format='delta',
        partition_by=['country'],
        merge_update_columns=[
            'host_id',
            'property_type',
            'room_type',
            'city',
            'country',
            'accommodates',
            'bedrooms',
            'bathrooms',
            'price_per_night',
            'effective_from',
            'effective_to',
            'is_current',
            'dbt_scd_id',
            'dbt_updated_at'
        ],
        post_hook="{{ update_control_watermark('dim_listing') }}",
        tags=['gold', 'dimension']
    )
}}

WITH snapshot_data AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'listing_id',
            'dbt_valid_from'
        ]) }}                                           AS listing_key,

        -- Natural Key
        listing_id,

        -- Foreign Key
        host_id,

        -- Attributes
        property_type,
        room_type,
        city,
        country,
        accommodates,
        bedrooms,
        bathrooms,
        price_per_night,

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

    FROM {{ ref('snp_listings') }}

    -- Only pick up new/changed records from snapshot
    WHERE dbt_updated_at > {{ get_control_watermark('dim_listing') }}
)

SELECT * FROM snapshot_data