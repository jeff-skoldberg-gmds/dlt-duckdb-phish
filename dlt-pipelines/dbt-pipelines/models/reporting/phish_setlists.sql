with setlists as (
    select *
    from
        {{ ref('stg_phishapi__setlists') }}
    where
        artist_name = 'Phish'
        and not is_excluded
),

setlist_songs as (
    select
        unique_id,
        show_id,
        show_date,
        song_id,
        song,
        is_jam,
        is_reprise,
        _set,
        position,
        sound_check_songs,
        is_original,
        1 as times_played,
        _set = '1' and position = 1 as is_show_opener,
        _set = '2' and position = MIN(position) over (partition by show_id, _set) as is_second_set_opener,
        _set = 'e' and position = MIN(position) over (partition by show_id, _set) as is_encore_opener,
        _set = 'e' as is_encore
    from setlists
),

eras as (select * from {{ ref('stg_phishapi__eras') }}),

calcs as (
    select
        show_date,
        row_number() over (order by show_date desc) - 1 as shows_ago,
        shows_ago = 0 as is_current_show
    from
        setlists
    group by show_date
),

final as (
    select
        setlists.*,
        calcs.shows_ago,
        calcs.is_current_show,
        eras.era,
        setlist_songs.is_show_opener as is_cur_show_opener,
        setlist_songs.is_second_set_opener as is_cur_second_set_opener,
        setlist_songs.is_encore_opener as is_cur_encore_opener,
        setlist_songs.is_encore as is_cur_encore
    from
        setlists inner join
        calcs on setlists.show_date = calcs.show_date left join
        eras on setlists.show_date between eras.start_date and coalesce(eras.end_date, '4000-12-13'::date) join
        setlist_songs on setlists.unique_id = setlist_songs.unique_id
)

select * from final
order by show_date desc, position
