{% snapshot accounts_snapshot %}
{#
    SCD2 on descriptive attributes only. balance is deliberately NOT a check
    column: it changes on every transaction (a fast-changing measure), so its
    history lives in fct_account_balance_changes instead of creating a new
    dimension version per transaction.
#}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='account_id',
      strategy='check',
      check_cols=['customer_id', 'account_type', 'currency'],
      hard_deletes='invalidate'
    )
}}

SELECT * FROM {{ ref('stg_accounts') }}

{% endsnapshot %}
