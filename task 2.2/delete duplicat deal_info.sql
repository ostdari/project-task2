
--создаем временную таблицу со всеми deal_rk из rd и rd2
create temp table union_deal_rk as 
select * 
from rd.deal_info di 
union 
select * 
from rd2.deal_info di2;

--создаем временную таблицу со всеми deal_rk и новыми значениями deal_rk
create temp table new_rk_di as 
SELECT *, ROW_NUMBER() OVER (order by effective_from_date) + (SELECT MAX(deal_rk) FROM union_deal_rk) AS new_deal_rk,
ROW_NUMBER() OVER (partition by deal_rk, effective_from_date order by effective_from_date)
FROM union_deal_rk;

--заменяем rk у дубликатов
--rd
update
	rd.deal_info di
set
	deal_rk = nrd.new_deal_rk
from
	new_rk_di nrd
where
	di.deal_rk = nrd.deal_rk
	and di.effective_from_date=nrd.effective_from_date
	and di.deal_num = nrd.deal_num
	and nrd.row_number > 1;

--rd2 из csv 
--заменяем rk у дубликатов
update
	rd2.deal_info di
set
	deal_rk = nrd.new_deal_rk
from
	new_rk_di nrd
where
	di.deal_rk = nrd.deal_rk
	and di.effective_from_date=nrd.effective_from_date
	and di.deal_num = nrd.deal_num
	and nrd.row_number > 1;