update
	rd.loan_holiday lh
set
	deal_rk = nrd.new_deal_rk
from
	new_rk_di nrd
where
	lh.deal_rk = nrd.deal_rk
	and lh.effective_from_date = nrd.effective_from_date
	and nrd.ROW_NUMBER > 1;