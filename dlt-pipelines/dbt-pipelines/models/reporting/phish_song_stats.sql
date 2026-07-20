with setlist_songs as (
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
    from {{ ref('phish_setlists') }}
),

gap as (select * from {{ ref('int_show_date_gap') }}),


songs as (
    select
        song_id,
        song,
        last_played_date,
        gap as current_gap,
        debut_date
    from
        {{ ref('int_songs_last_played') }}
),

max_opener_date as (
    select
        song_id,
        MAX(show_date) as max_show_date,
        MIN(show_date) as min_show_date
    from
        setlist_songs
    where is_show_opener
    group by 1
),

max_second_set_opener_date as (
    select
        song_id,
        MAX(show_date) as max_show_date,
        MIN(show_date) as min_show_date
    from
        setlist_songs
    where is_second_set_opener
    group by 1
),

max_encore_date as (
    select
        song_id,
        true as is_encore,
        MAX(show_date) as max_show_date,
        MIN(show_date) as min_show_date
    from
        setlist_songs
    where is_encore
    group by 1
),

max_encore_opener_date as (
    select
        song_id,
        MAX(show_date) as max_show_date,
        MIN(show_date) as min_show_date
    from
        setlist_songs
    where is_encore_opener
    group by 1
),

joined as (
    select
        setlist_songs.song,
        setlist_songs.song_id,
        setlist_songs.is_show_opener,
        setlist_songs.is_second_set_opener,
        setlist_songs.is_encore_opener,
        setlist_songs.is_encore,
        setlist_songs.times_played,
        songs.last_played_date,
        songs.current_gap,
        songs.debut_date,
        max_opener_date.max_show_date as max_opener_date,
        max_opener_date.min_show_date as min_opener_date,
        max_second_set_opener_date.max_show_date as max_second_set_opener_date,
        max_second_set_opener_date.min_show_date as min_second_set_opener_date,
        max_encore_date.max_show_date as max_encore_date,
        max_encore_date.min_show_date as min_encore_date,
        max_encore_opener_date.max_show_date as max_encore_opener_date,
        max_encore_opener_date.min_show_date as min_encore_opener_date,
        opener_gap.gap as gap_opener,
        second_opener_gap.gap as gap_second_opener,
        encore_opener_gap.gap as gap_encore_opener,
        encore_gap.gap as gap_encore


    from
        setlist_songs inner join
        songs on setlist_songs.song_id = songs.song_id left join
        max_encore_date on songs.song_id = max_encore_date.song_id left join
        max_opener_date on songs.song_id = max_opener_date.song_id left join
        max_encore_opener_date on songs.song_id = max_encore_opener_date.song_id left join
        max_second_set_opener_date on songs.song_id = max_second_set_opener_date.song_id left join
        gap as opener_gap on max_opener_date.max_show_date = opener_gap.show_date left join
        gap as second_opener_gap on max_second_set_opener_date.max_show_date = second_opener_gap.show_date left join
        gap as encore_opener_gap on max_encore_opener_date.max_show_date = encore_opener_gap.show_date left join
        gap as encore_gap
        on max_encore_date.max_show_date = encore_gap.show_date


),

metrics as (
    select
        song_id,
        -- There are two songs (Gloria and Let's Go) that actaully have two seperate song IDs
        LISTAGG(distinct song, '|') as song,

        --generic stats
        SUM(times_played) as times_played,
        MAX(last_played_date) as last_played_date,
        min(debut_date) as debut_date,
        min(current_gap) as current_gap,


        -- show opener fields
        MAX(is_show_opener) as is_show_opener,
        MAX(max_opener_date) as last_opener_date,
        Min(min_opener_date) as first_opener_date,
        SUM(is_show_opener::int) as show_opener_count,
        MIN(gap_opener) as gap_opener,

        -- second set opener fields
        MAX(is_second_set_opener) as is_second_set_opener,
        MAX(max_second_set_opener_date) as last_second_set_opener_date,
        Min(min_second_set_opener_date) as first_second_set_opener_date,
        SUM(is_second_set_opener::int) as second_set_opener_count,
        MIN(gap_second_opener) as gap_second_opener,

        --encore opener fields
        MAX(is_encore_opener) as is_encore_opener,
        MAX(max_encore_opener_date) as last_encore_opener_date,
        min(min_encore_opener_date) as first_encore_opener_date,
        SUM(is_encore_opener::int) as encore_opener_count,
        MIN(gap_encore_opener) as gap_encore_opener,

        --encore fields
        MAX(is_encore) as is_encore,
        MAX(max_encore_date) as last_encore_date,
        min(min_encore_date) as first_encore_date,
        SUM(is_encore::int) as encore_count,
        MIN(gap_encore) as gap_encore

    from
        joined
    group by 1
)

select * from metrics
