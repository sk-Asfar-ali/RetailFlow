{% snapshot customers_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'phone',
                    'address_line', 'city', 'state', 'country',
                    'postal_code', 'is_active'],
        invalidate_hard_deletes=True,
    )
}}

-- SCD Type 2 snapshot of the customers dimension, using dbt's `check`
-- strategy: dbt computes a hash across `check_cols` on every run and
-- compares it to the last captured version. Any difference -- even one
-- not reflected in `updated_at` -- triggers a new version. More robust
-- than the `timestamp` strategy, which trusts `updated_at` completely
-- and would silently miss a change if that column were ever wrong or
-- bypassed (e.g. a manual SQL fix, a bulk import).
--
-- Produces dbt_valid_from / dbt_valid_to columns automatically -- query
-- this table to answer "what did this customer's profile look like on
-- date X" or "how many times has this customer moved".

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address_line,
    city,
    state,
    country,
    postal_code,
    signup_date,
    is_active,
    updated_at
from {{ source('bronze', 'customers') }}

{% endsnapshot %}