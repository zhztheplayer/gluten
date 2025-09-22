
select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
from store_returns
where sr_customer_sk = 102
and sr_store_sk = 1


