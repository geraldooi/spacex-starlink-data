with launches as (
    select 
        *
    from {{ source('bronze', 'raw_launches') }}
)

select
    id as launch_id,
    {{ dbt.safe_cast('date_utc', api.Column.translate_type('date')) }} as launch_date,
    {{ format_date_as_string('date_utc') }} as launch_date_key,
    name as launch_name
from launches