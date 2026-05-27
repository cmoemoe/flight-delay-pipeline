with enriched as (
    select * from {{ ref('int_flights_enriched') }}
)

select
    date_trunc(flight_date, month)                                              as flight_month,
    origin_airport_code,
    origin_airport_name,
    origin_city,
    origin_state,
    origin_lat,
    origin_lon,
    count(*)                                                                    as total_departures,
    countif(is_delayed)                                                         as delayed_departures,
    round(countif(is_delayed) / count(*) * 100, 1)                             as delay_rate_pct,
    round(avg(case when is_delayed then dep_delay_min end), 1)                 as avg_delay_min,
    case
        when sum(late_aircraft_delay_min) >= greatest(
               sum(carrier_delay_min), sum(weather_delay_min), sum(nas_delay_min))
        then 'Late aircraft'
        when sum(carrier_delay_min) >= greatest(
               sum(weather_delay_min), sum(nas_delay_min))
        then 'Carrier'
        when sum(weather_delay_min) >= sum(nas_delay_min) then 'Weather'
        else 'NAS / ATC'
    end                                                                         as dominant_delay_cause
from enriched
group by 1, 2, 3, 4, 5, 6, 7
order by flight_month desc, delay_rate_pct desc
