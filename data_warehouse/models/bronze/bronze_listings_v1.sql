{{
      config(
        materialized='incremental',
        incremental_strategy='append',
        catalog = 'airbnb_data_hub_bronze',
        schema = 'airbnb_bronze_db',
        file_format = 'delta',
        partition_by = ["_bronze_load_date"]
        )
}}

with source_data as (
        select 
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
            current_timestamp()           AS _bronze_loaded_at,
            current_date()                AS _bronze_load_date,
            year(current_date() )   as year,
            month(current_date() )  as month,
            day(current_date() )    as day
        from {{ source('stage', 'listings') }}
)

select 
*
from source_data
if {% if is_incremental() %}
    where created_at > ( select coalesce(max(created_at), '1900-01-01') from {{ this }} )
{% endif %}