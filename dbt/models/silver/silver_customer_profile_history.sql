-- Silver: demonstrates practical use of the customers SCD2 snapshot --
-- counts how many times each customer's profile has changed (e.g. moved
-- address), and flags customers with a history of multiple addresses.
-- This is the kind of question only answerable because of SCD2 history;
-- the current-state `silver_customers` table alone cannot answer it.

with snapshot_history as (

    select * from {{ ref('customers_snapshot') }}

),

change_counts as (

    select
        customer_id,
        count(*) as total_profile_versions,
        count(*) - 1 as number_of_profile_changes,
        min(dbt_valid_from) as first_seen_at,
        max(case when dbt_valid_to is null then dbt_valid_from end) as current_version_since

    from snapshot_history
    group by customer_id

)

select
    customer_id,
    total_profile_versions,
    number_of_profile_changes,
    first_seen_at,
    current_version_since,
    case when number_of_profile_changes > 0 then true else false end as has_relocated

from change_counts
