-- Every transaction amount must be strictly positive (mirrors the Postgres CHECK).
select transaction_id, amount
from {{ ref('fact_transactions') }}
where amount <= 0
