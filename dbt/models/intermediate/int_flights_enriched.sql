-- Intermediate: join carrier and airport seed lookups onto cleaned flights
with flights as (
    select * from {{ ref('stg_flights') }}
),
carriers as (
    select * from {{ ref('carriers') }}
),
origin_airports as (
    select * from {{ ref('airports') }}
),
dest_airports as (
    select * from {{ ref('airports') }}
)

select
    f.*,
    c.carrier_name,
    c.carrier_group,

    oa.airport_name  as origin_airport_name,
    oa.latitude      as origin_lat,
    oa.longitude     as origin_lon,

    da.airport_name  as dest_airport_name,
    da.latitude      as dest_lat,
    da.longitude     as dest_lon

from flights f
left join carriers        c  on f.c