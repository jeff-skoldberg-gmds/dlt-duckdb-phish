with songs as (select * from {{ source('PHISH', 'SONGS') }}),

renamed as (
    select
        songid as song_id,
        song,
        debut,
        artist,
        gap,
        last_played::date as last_played,
        abbr as abbreviation,
        slug,
        debut_permalink,
        last_permalink
    from songs
)

select * from renamed
