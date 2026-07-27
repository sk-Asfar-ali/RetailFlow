-- Gold: clickstream conversion funnel. Counts distinct sessions reaching
-- each stage of the funnel: page_view -> product_view -> add_to_cart -> checkout_start.
-- Also computes conversion rate between each consecutive stage.

with sessions as (

    select * from {{ ref('silver_clickstream_sessions') }}

),

session_stage_flags as (

    select
        session_id,
        max(case when event_type = 'page_view' then 1 else 0 end) as reached_page_view,
        max(case when event_type = 'product_view' then 1 else 0 end) as reached_product_view,
        max(case when event_type = 'add_to_cart' then 1 else 0 end) as reached_add_to_cart,
        max(case when event_type = 'checkout_start' then 1 else 0 end) as reached_checkout,
        max(case when is_anonymous_session then 1 else 0 end) as is_anonymous

    from sessions
    group by session_id

),

funnel_counts as (

    select
        sum(reached_page_view) as sessions_page_view,
        sum(reached_product_view) as sessions_product_view,
        sum(reached_add_to_cart) as sessions_add_to_cart,
        sum(reached_checkout) as sessions_checkout,
        sum(is_anonymous) as anonymous_sessions,
        count(*) as total_sessions

    from session_stage_flags

),

final as (

    select
        total_sessions,
        anonymous_sessions,
        (total_sessions - anonymous_sessions) as identified_sessions,

        sessions_page_view,
        sessions_product_view,
        sessions_add_to_cart,
        sessions_checkout,

        round(sessions_product_view / nullif(sessions_page_view, 0) * 100, 2) as pct_page_to_product,
        round(sessions_add_to_cart / nullif(sessions_product_view, 0) * 100, 2) as pct_product_to_cart,
        round(sessions_checkout / nullif(sessions_add_to_cart, 0) * 100, 2) as pct_cart_to_checkout,
        round(sessions_checkout / nullif(sessions_page_view, 0) * 100, 2) as pct_overall_conversion

    from funnel_counts

)

select * from final
