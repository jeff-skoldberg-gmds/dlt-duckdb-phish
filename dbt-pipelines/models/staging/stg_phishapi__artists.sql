with
artists as (select * from {{ source('PHISH', 'ARTISTS') }}),

renamed as (
    select
        id::int as artist_id,
        artist as artist,
        slug as slug
    from artists
)

select * from renamed
