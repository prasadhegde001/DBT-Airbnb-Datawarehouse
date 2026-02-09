{% snapshot snp_hosts %}

{{
    config(
        target_database='airbnb_data_hub_gold',
        target_schema='airbnb_gold_snapshot',
        unique_key='host_id',
        strategy='check',
        check_cols=[
            'host_name',
            'is_superhost',
            'response_rate'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
    host_id,
    host_name,
    host_since,
    is_superhost,
    response_rate,
    created_at,
    _silver_loaded_at
FROM {{ ref('silver_hosts') }}

{% endsnapshot %}
