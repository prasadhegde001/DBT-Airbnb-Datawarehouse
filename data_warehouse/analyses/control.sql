SELECT
            *
        FROM {{ source('gold', 'control_table') }}
