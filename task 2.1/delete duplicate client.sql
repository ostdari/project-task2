
WITH ranked_clients AS (
    SELECT 
           client_rk, 
           effective_from_date,
           ROW_NUMBER() OVER (
               PARTITION BY client_rk, effective_from_date 
           ) AS rn
    FROM dm.client
)
DELETE FROM dm.client c
USING ranked_clients rc
WHERE c.client_rk=rc.client_rk and 
           c.effective_from_date = rc.effective_from_date
AND rc.rn > 1;