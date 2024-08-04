CREATE UNIQUE INDEX unique_product ON rd.product (product_rk, effective_from_date);

INSERT INTO rd.product
SELECT *
FROM rd2.product
ON CONFLICT (product_rk, effective_from_date) 
DO UPDATE SET
  product_name = EXCLUDED.product_name,
  effective_to_date = EXCLUDED.effective_to_date;

CREATE UNIQUE INDEX unique_deal ON rd.deal_info (deal_rk, effective_from_date);

INSERT INTO rd.deal_info 
SELECT *
FROM rd2.deal_info 
ON CONFLICT (deal_rk, effective_from_date) 
DO UPDATE set
deal_num = EXCLUDED.deal_num, 
deal_name= EXCLUDED.deal_name,
deal_sum= EXCLUDED.deal_sum,
client_rk= EXCLUDED.client_rk,
account_rk= EXCLUDED.account_rk,
agreement_rk= EXCLUDED.agreement_rk,
deal_start_date= EXCLUDED.deal_start_date,
department_rk= EXCLUDED.department_rk,
product_rk= EXCLUDED.product_rk,
deal_type_cd= EXCLUDED.deal_type_cd,
effective_from_date= EXCLUDED.effective_from_date,
effective_to_date= EXCLUDED.effective_to_date;