with starlink as (
    select 
        *
    from {{ source('staging', 'starlink_bronze') }}
)

select
    id,
    launch
from starlink