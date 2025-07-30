with source as (
    select 
        *
    from {{ source('bronze', 'raw_starlink') }}
),

starlinks as (
    select
        id as starlink_id,
        launch as launch_id,
        "spaceTrack.OBJECT_ID" as object_code,
        "spaceTrack.OBJECT_NAME" as object_name,
        {{ dbt.safe_cast('"spaceTrack.LAUNCH_DATE"', api.Column.translate_type('date')) }} as launch_date,
        {{ format_date_as_string('"spaceTrack.LAUNCH_DATE"') }} as launch_date_key, 
        {{ dbt.safe_cast('"spaceTrack.CREATION_DATE"', api.Column.translate_type('date')) }} as creation_date,
        {{ format_date_as_string('"spaceTrack.CREATION_DATE"') }} as creation_date_key,
        {{ get_decay_indicator('"spaceTrack.DECAYED"') }} as decay_indicator,
        {{ dbt.safe_cast('"spaceTrack.DECAY_DATE"', api.Column.translate_type('date')) }} as decay_date,
        {{ format_date_as_string('"spaceTrack.DECAY_DATE"') }} as decay_date_key
    from source
),

starlinks_clean as (
    select 
        starlink_id,
        launch_id,
        object_code,
        object_name,
        launch_date,
        launch_date_key,
        creation_date,
        creation_date_key,
        decay_indicator,
        decay_date,
        decay_date_key
    from starlinks
    where launch_date is not null -- drop the unknown launch starlink snapshot row
)

select * from starlinks_clean
