select "SHOW_DATE", "POSITION","_SET"

FROM

{{ ref('phish_setlists') }}

where "SHOWS_AGO" < 3
order by 1 desc,2
