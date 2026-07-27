-- Silver: clickstream events enriched with session-level sequencing.
-- Orders events within each session chronologically, and computes time
-- since the previous event in the same session -- useful downstream for
-- funnel analysis (page_view -> product_view -> add_to_cart -> checkout).

with source as (

    select * from {{ source('bronze', 'clickstream_events') }}
    where event_time is not null   -- drop rows where timestamp parsing failed upstream

),

sequenced as (

    select
        event_id,
        lower(trim(event_type)) as event_type,   -- normalize casing: bronze has mixed PAGE_VIEW/page_view etc.
        event_time,
        session_id,
        customer_id,
        device,
        page_url,
        referrer,
        user_agent,
        ip_address,
        product_id,
        quantity,
        search_term,
        cart_value,

        row_number() over (
            partition by session_id
            order by event_time
        ) as event_sequence_in_session,

        lag(event_time) over (
            partition by session_id
            order by event_time
        ) as previous_event_time,

        lag(lower(trim(event_type))) over (
            partition by session_id
            order by event_time
        ) as previous_event_type

    from source

),

enriched as (

    select
        event_id,
        lower(trim(event_type)) as event_type,
        event_time,
        session_id,
        customer_id,
        device,
        page_url,
        referrer,
        user_agent,
        ip_address,
        product_id,
        quantity,
        search_term,
        cart_value,
        event_sequence_in_session,
        previous_event_type,
        case
            when previous_event_time is not null
            then (unix_timestamp(event_time) - unix_timestamp(previous_event_time))
            else null
        end as seconds_since_previous_event,
        case when customer_id is null then true else false end as is_anonymous_session

    from sequenced

)

select * from enriched