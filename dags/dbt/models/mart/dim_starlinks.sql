with starlinks as (
    select
        starlink_id,
        decay_date_key,
        decay_indicator,
        object_code,
        object_name
    from {{ ref('stg_spacex__starlinks') }}
)

select * from starlinks