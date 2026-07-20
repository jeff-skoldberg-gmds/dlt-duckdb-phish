with venues as (select * from {{ source('PHISH', 'VENUES') }}),

renamed as (
    select
        venueid::int as venue_id,
        venuename as venue_name,
        short_name,
        city,
        state,
        country,
        alias,
        venuenotes as venue_notes
    from
        venues
)

select * from renamed
