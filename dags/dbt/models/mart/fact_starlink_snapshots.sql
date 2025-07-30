with starlinks as (
    select 
        starlink_id,
        launch_id,
        creation_date_key 
    from {{ ref('stg_spacex__starlinks') }}
)

select * from starlinks