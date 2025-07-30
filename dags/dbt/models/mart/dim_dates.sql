with 

starlinks as (
    select * from {{ ref('stg_spacex__starlinks') }}
),

launches as (
    select * from {{ ref('stg_spacex__launches') }}
),

starlinks_creation_date as (
    select
        creation_date_key as date_key,
        creation_date as iso_date
    from starlinks
),

starlinks_launch_date as (
    select
        launch_date_key as date_key,
        launch_date as iso_date
    from starlinks
    where launch_date is not null
),

starlinks_decay_date as (
    select
        decay_date_key as date_key,
        decay_date as iso_date
    from starlinks
    where decay_date is not null
),

launches_date as (
    select
        launch_date_key as date_key,
        launch_date as iso_date
    from launches
),

union_all as (
    select 
        *
    from (
        select * from starlinks_creation_date
        union all 
        select * from starlinks_launch_date
        union all 
        select * from starlinks_decay_date
        union all 
        select * from launches_date
    ) union_tbl
),

unique_date as (
    select distinct
        date_key,
        iso_date
    from union_all
)

select 
    date_key,
    iso_date,
    EXTRACT(YEAR FROM iso_date) calendar_year,
    EXTRACT(QUARTER FROM iso_date) calendar_quarter,
    EXTRACT(MONTH FROM iso_date) calendar_month,
    EXTRACT(DAY FROM iso_date) day_of_month
from unique_date
