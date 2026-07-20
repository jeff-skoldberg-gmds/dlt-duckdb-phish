with duplicate_ids as 
(
select  
SONGID,
count(*) cnt
from 
{{ source('PHISH', 'SONGS') }}
GROUP BY SONGID
having count(SONGID) > 1
)

select * 
from {{ source('PHISH', 'SONGS') }} a
join duplicate_ids b on a.SONGID = b.SONGID
