-- Silver: thin convenience view over products_snapshot exposing an
-- explicit `is_current` boolean, plus `is_discontinued` already carries
-- the soft-delete signal -- a product can be both "current" (this is its
-- latest known state) and "discontinued" (it's been soft-deleted).

select
    *,
    (dbt_valid_to is null) as is_current

from {{ ref('products_snapshot') }}