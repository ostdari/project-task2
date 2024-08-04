with account_balance_with_corrections as (
select
	a.account_rk,
	ab.effective_date,
	ab.account_in_sum,
	ab.account_out_sum,
	lag(ab.account_out_sum) over (partition by a.account_rk order by
	ab.effective_date) as prev_day_out_sum,
	case
		when ab.account_in_sum <> lag(ab.account_out_sum) over (partition by a.account_rk order by ab.effective_date) 
            then lag(ab.account_out_sum) over (partition by a.account_rk order by ab.effective_date)
		else null
	end as corrected_account_in_sum
from rd.account a
left join rd.account_balance ab on
	a.account_rk = ab.account_rk)
update
	rd.account_balance ab
set
	account_in_sum = ac.corrected_account_in_sum
from
	account_balance_with_corrections ac
where
	ab.account_rk = ac.account_rk
	and ab.effective_date = ac.effective_date
	and ac.corrected_account_in_sum is not null;
