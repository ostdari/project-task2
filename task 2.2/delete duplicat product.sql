--создаем временную таблицу со всеми product_rk из rd и rd2
create temp table union_rk as 
select * 
from rd.product p 
union 
select * 
from rd2.product p;

--создаем временную таблицу со всеми product_rk и новыми значениями product_rk
create temp table new_rk as 
SELECT *, ROW_NUMBER() OVER (order by effective_from_date) + (SELECT MAX(product_rk) FROM union_rk) AS new_product_rk,
ROW_NUMBER() OVER (partition by product_rk, effective_from_date order by effective_from_date)
FROM union_rk;

--заменяем rk у дубликатов
--rd
update
	rd.product p
set
	product_rk = nk.new_product_rk
from
	new_rk nk
where
	p.product_rk = nk.product_rk
	and p.product_name = nk.product_name
	and p.effective_from_date =nk.effective_from_date
	and p.effective_to_date = nk.effective_to_date
	and nk.row_number > 1;

--rd2 из csv 
update
	rd2.product p
set
	product_rk = nk.new_product_rk
from
	new_rk nk
where
	p.product_rk = nk.product_rk
	and p.product_name = nk.product_name
	and p.effective_from_date =nk.effective_from_date
	and p.effective_to_date = nk.effective_to_date
	and nk.row_number > 1;   

--deal_info
update
	rd.deal_info di
set
	product_rk = nk.new_product_rk
from
	new_rk nk
where
	di.product_rk = nk.product_rk
	and di.deal_name = nk.product_name
	and di.effective_from_date =nk.effective_from_date
	and di.effective_to_date = nk.effective_to_date
	and nk.row_number > 1;

--deal_info rd2
update
	rd2.deal_info di
set
	product_rk = nk.new_product_rk
from
	new_rk nk
where
	di.product_rk = nk.product_rk
	and di.deal_name = nk.product_name
	and di.effective_from_date =nk.effective_from_date
	and di.effective_to_date = nk.effective_to_date
	and nk.row_number > 1;