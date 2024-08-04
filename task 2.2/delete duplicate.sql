--product

with ranked_product as (
select
	*,
	row_number() over(partition by p.product_rk,
	p.product_name,
	p.effective_from_date,
	p.effective_to_date
           ) as rn
from
	rd.product p
)
delete
from
	rd.product p
		using ranked_product rp
where
	p.product_rk = rp.product_rk
	and p.product_name = rp.product_name
	and p.effective_from_date = rp.effective_from_date
	and p.effective_to_date = rp.effective_to_date
	and rp.rn > 1;

	--product из csv 

with ranked_product as (
select
	*,
	row_number() over(partition by p.product_rk,
	p.product_name,
	p.effective_from_date,
	p.effective_to_date
           ) as rn
from
	rd2.product p
)
delete
from
	rd2.product p
		using ranked_product rp
where
	p.product_rk = rp.product_rk
	and p.product_name = rp.product_name
	and p.effective_from_date = rp.effective_from_date
	and p.effective_to_date = rp.effective_to_date
	and rp.rn > 1;