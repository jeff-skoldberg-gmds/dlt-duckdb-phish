with shows as (
    select *
    from {{ source('PHISH', 'SHOWS') }}
),

renamed as (
    select
        showid::int as show_id,
        tourid as tour_id,
        venueid as venue_id,
        venue,
        country,
        showyear::int as show_year,
        city,
        artist_name,
        tour_name,
        showday::int as show_day,
        artistid::int as artist_id,
        showmonth::int as show_month,
        updated_at,
        exclude_from_stats::boolean as is_excluded_from_stats,
        state,
        setlist_notes,
        permalink,
        showdate::date as show_date
    from shows
)

select *
from renamed
