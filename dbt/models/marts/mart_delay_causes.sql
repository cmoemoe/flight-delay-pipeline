with enriched as (
    select * from {{ ref('int_flights_enriched') }}
),

monthly_causes as (
    select
        date_trunc(flight_date, month)         as flight_month,
        round(sum(carrier_delay_min), 0)       as carrier_delay_min,
        round(sum(weather_delay_min), 0)       as weather_delay_min,
        round(sum(nas_delay_min), 0)           as nas_delay_min,
        round(sum(security_delay_min), 0)      as security_delay_min,
        round(sum(late_aircraft_delay_min), 0) as late_aircraft_delay_min
    from enriched
    where is_delayed
    group by 1
)

select
    *,
    (carrier_delay_min + weather_delay_min + nas_delay_min
     + security_delay_min + late_aircraft_delay_min) as total_delay_min,
    round(late_aircraft_delay_min / nullif(
        carrier_delay_min + weather_delay_min + nas_delay_min
        + security_delay_min + late_aircraft_delay_min, 0) * 100, 1) as late_aircraft_pct,
    round(carrier_delay_min / nullif(
        carrier_delay_min + weather_delay_min + nas_delay_min
        + security_delay_min + late_aircraft_delay_min, 0) * 100, 1) as carrier_pct,
    round(weather_delay_min / nullif(
        carrier_delay_min + weather_delay_min + nas_delay_min
        + security_delay_min + late_aircraft_delay_min, 0) * 100, 1) as weather_pct,
    round(nas_delay_min / nullif(
        carrier_delay_min + weather_delay_min + nas_delay_min
        + security_delay_min + late_aircraft_delay_min, 0) * 100, 1) as nas_pct
from monthly_causes
order by flight_month
