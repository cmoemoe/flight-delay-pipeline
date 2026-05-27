-- Staging: clean types, rename BTS columns, derive flags, filter diverted
with source as (
    select * from {{ source('flights_raw', 'flights') }}
),

renamed as (
    select
        cast(FlightDate as date)                         as flight_date,
        trim(Reporting_Airline)                          as carrier_code,
        trim(Tail_Number)                                as tail_number,
        cast(Flight_Number_Reporting_Airline as string)  as flight_number,
        trim(Origin)                                     as origin_airport_code,
        trim(OriginCityName)                             as origin_city,
        trim(OriginState)                                as origin_state,
        trim(Dest)                                       as dest_airport_code,
        trim(DestCityName)                               as dest_city,
        trim(DestState)                                  as dest_state,
        cast(CRSDepTime as int64)                        as scheduled_dep_time,
        cast(DepTime as int64)                           as actual_dep_time,
        cast(DepDelay as float64)                        as dep_delay_min,
        cast(DepDelayMinutes as float64)                 as dep_delay_min_abs,
        cast(CRSArrTime as int64)                        as scheduled_arr_time,
        cast(ArrTime as int64)                           as actual_arr_time,
        cast(ArrDelay as float64)                        as arr_delay_min,
        cast(ArrDelayMinutes as float64)                 as arr_delay_min_abs,
        case when Cancelled = 1 then true else false end as is_cancelled,
        nullif(trim(CancellationCode), '')               as cancellation_code,
        cast(CarrierDelay as float64)                    as carrier_delay_min,
        cast(WeatherDelay as float64)                    as weather_delay_min,
        cast(NASDelay as float64)                        as nas_delay_min,
        cast(SecurityDelay as float64)                   as security_delay_min,
        cast(LateAircraftDelay as float64)               as late_aircraft_delay_min,
        cast(AirTime as float64)                         as air_time_min,
        cast(Distance as float64)                        as distance_miles
    from source
    where Diverted = 0 or Diverted is null
),

with_flags as (
    select
        *,
        case when dep_delay_min > 15 then true else false end as is_delayed,
        case
            when dep_delay_min is null or dep_delay_min <= 0 then 'on_time'
            when dep_delay_min <= 15                          then 'minor'
            when dep_delay_min <= 30                          then 'moderate'
            when dep_delay_min <= 60                          then 'significant'
            when dep_delay_min <= 120                         then 'severe'
            else                                                   'extreme'
        end                                                   as delay_category
    from renamed
)

select * from with_flags
