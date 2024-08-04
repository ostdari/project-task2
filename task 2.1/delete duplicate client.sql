with ranked_clients as (
select
	client_rk,
	effective_from_date,
	row_number() over (
               partition by client_rk,
	effective_from_date 
           ) as rn
from
	dm.client
)
delete from dm.client c
using ranked_clients rc
where
	c.client_rk = rc.client_rk
	and 
           c.effective_from_date = rc.effective_from_date
	and rc.rn > 1;
