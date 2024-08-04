
--задание 1
with account_balance as (
select
	a.account_rk,
	coalesce(dc.currency_name, '-1') as currency_name,
	a.department_rk,
	ab.effective_date,
	ab.account_in_sum,
	ab.account_out_sum,
	lag(ab.account_out_sum) over (partition by a.account_rk
order by ab.effective_date) as prev_day_out_sum
from rd.account a
left join rd.account_balance ab on
	a.account_rk = ab.account_rk
left join dm.dict_currency dc on
	a.currency_cd = dc.currency_cd
)
select
	account_rk,
	currency_name,
	department_rk,
	effective_date,
	case
		when account_in_sum <> prev_day_out_sum then prev_day_out_sum
		else account_in_sum
	end as corrected_account_in_sum,
	account_out_sum
from account_balance;


-- задание 2

with account_balance2 as (
select
	a.account_rk,
	coalesce(dc.currency_name, '-1') as currency_name,
	a.department_rk,
	ab.effective_date,
	ab.account_in_sum,
	ab.account_out_sum,
	lead(ab.account_in_sum) over (partition by ab.account_rk
order by ab.effective_date) as next_day_in_sum
from rd.account a
left join rd.account_balance ab on
	a.account_rk = ab.account_rk
left join dm.dict_currency dc on
	a.currency_cd = dc.currency_cd
)
select
	account_rk,
	currency_name,
	department_rk,
	effective_date,
	account_in_sum,
	case
		when account_out_sum <> next_day_in_sum then next_day_in_sum
		else account_out_sum
	end as corrected_account_out_sum
from account_balance2;
