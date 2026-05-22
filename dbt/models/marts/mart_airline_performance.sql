-- Mart: monthly carrier scorecard → Power BI airline performance page
with enriched as (
    select * from {{ ref('int_flights_enriched') }}
),

monthly as (
    select
        date_trunc(flight_date, month)                                           as flight_month,
        carrier_code,
        carrier_name,
        carrier_group,

        count(*)                                                                 as total_flights,
        countif(is_cancelled)                                                    as cancelled_flights,
        countif(is_delayed)                                                      as delayed_flights,
        countif(not is_delayed and not is_cancelled)                             as on_time_flights,

        round(countif(not is_delayed and not is_cancelled) / count(*) * 100, 1) as on_time_pct,
        round(countif(is_cancelled) / count(*) * 100, 2)                        as cancellation_pct,

        round(avg(case when is_delayed then dep_delay_min end), 1)              as avg_delay_min_delayed,

        round(sum(carrier_delay_min), 0)                                         as total_carrier_delay_min,
        round(sum(weather_delay_min), 0)                                         as total_weather_delay_min,
        round(sum(nas_delay_min), 0)                                             as total_nas_delay_min,
        round(sum(security_delay_min), 0)                                        as total_security_delay_min,
        round(sum(late_aircraft_delay_min), 0)                                   as total_late_aircraft_delay_min

    from enriched
    group by 1, 2, 3, 4
)

select
    *,
    round(
        on_time_pct - lag(on_time_pct) over (
            partition by carrier_code order by flight_month
        ), 1
    ) as on_time_pct_mom_change
from monthly
order by flight_month desc, on_time_pct desc