with launches as (
    select
        launch_id,
        launch_date_key,
        launch_name
    from {{ ref('stg_spacex__launches') }}
)

select * from launches