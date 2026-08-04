-- Silver: thin convenience view over customers_snapshot exposing an
-- explicit `is_current` boolean, since `dbt_valid_to IS NULL` isn't
-- immediately obvious to someone querying the table without knowing
-- dbt snapshot conventions.

select
    *,
    (dbt_valid_to is null) as is_current

from {{ ref('customers_snapshot') }}