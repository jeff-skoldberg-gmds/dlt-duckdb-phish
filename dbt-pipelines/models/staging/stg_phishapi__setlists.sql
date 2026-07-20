with setlists as (
    select *
    from
        {{ source('PHISH', 'SETLISTS') }}
),

renamed as (
    select
        uniqueid::int as unique_id,
        songid::int as song_id,
        showid::int as show_id,
        tourid::int as tour_id,
        artistid::int as artist_id,
        venueid::int as venue_id,
        song,
        position::int as position,
        "SET" as _set,
        tourname as tour_name,
        tourwhen as tour_when,
        venue,
        city,
        state,
        country,
        showdate::date as show_date,
        showyear,
        setlistnotes,
        artist_name,
        isjamchart::boolean as is_jame_chart,
        soundcheck as sound_check_songs,
        is_original::boolean as is_original,
        exclude::boolean as is_excluded,
        gap,
        transition,
        tracktime,
        nickname,
        isreprise as is_reprise,
        meta,
        jamchart_description,
        reviews,
        artist_slug,
        permalink,
        trans_mark,
        isjam as is_jam,
        footnote,
        slug,
        1 as row_count
    from
        setlists
)

select * from renamed

