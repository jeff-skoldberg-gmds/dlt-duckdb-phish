{{ config(materialized='table') }}

with setlists as (
    select *
    from
        {{ ref('stg_phishapi__setlists') }}
    where
        artist_name = 'Phish'
        and not is_excluded
),

calcs as (
    select
        show_date,
        row_number() over (order by show_date desc) - 1 as gap
    from
        setlists
    group by show_date
)

select * from calcs
order by show_date desc
