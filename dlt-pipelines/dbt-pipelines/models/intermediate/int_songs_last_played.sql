with setlists as (
    select *
    from
        {{ ref('stg_phishapi__setlists') }}
    where
        artist_name = 'Phish'
        and not is_excluded
),

gap as (select * from {{ ref('int_show_date_gap') }}),

max_date as (
    select
        song_id,
        max(show_date) as last_played_date,
        listagg(distinct song,'|') as song,  -- there are two songs that are not unique
        count(1) as times_played
    from setlists
    group by
        all
),

min_date as (
    select
        song_id,
        min(show_date) as debut_date
    from setlists
    group by
        all
),

joined as (
    select
        max_date.song_id,
        max_date.song,
        max_date.last_played_date,
        max_date.times_played,
        min_date.debut_date,
        gap.gap
    from
        max_date join
        min_date on max_date.song_id = min_date.song_id join
        gap on max_date.last_played_date = gap.show_date

)

select * from joined
