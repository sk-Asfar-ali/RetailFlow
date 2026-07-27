-- Gold: daily sales summary. One row per calendar day, aggregating order
-- volume, revenue, and status breakdown -- the classic "exec dashboard" table.

with orders as (

    select * from {{ ref('silver_orders') }}

),

daily as (

    select
        date(order_date) as order_day,

        count(distinct order_id) as total_orders,
        count(distinct case when status = 'DELIVERED' then order_id end) as delivered_orders,
        count(distinct case when status = 'CANCELLED' then order_id end) as cancelled_orders,
        count(distinct case when status in ('PENDING', 'CONFIRMED', 'SHIPPED') then order_id end) as in_flight_orders,

        sum(total_amount) as gross_revenue,
        sum(case when status != 'CANCELLED' then total_amount else 0 end) as net_revenue,

        count(distinct customer_id) as unique_customers,
        round(avg(total_amount), 2) as avg_order_value

    from orders
    group by date(order_date)

)

select * from daily
order by order_day
