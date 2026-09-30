-- A TRANSFER must have a counterparty different from the sender;
-- DEPOSIT / WITHDRAWAL must not have one.
select transaction_id, transaction_type, account_id, related_account_id
from {{ ref('fact_transactions') }}
where (transaction_type = 'TRANSFER' and (related_account_id is null or related_account_id = account_id))
   or (transaction_type <> 'TRANSFER' and related_account_id is not null)
