-- Gold: customer lifetime value and purchasing behavior summary.
-- One row per customer, joining their profile with aggregated order history.

with customers as (

    select * from {{ ref('silver_customers') }}

),

orders as (

    select * from {{ ref('silver_orders') }}

),

payments as (

    select * from {{ ref('silver_payments') }}

),

customer_orders as (

    select
        customer_id,
        count(distinct order_id) as total_orders,
        count(distinct case when status = 'DELIVERED' then order_id end) as delivered_orders,
        count(distinct case when status = 'CANCELLED' then order_id end) as cancelled_orders,
        sum(case when status != 'CANCELLED' then total_amount else 0 end) as lifetime_revenue,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date

    from orders
    group by customer_id

),

customer_payment_health as (

    select
        o.customer_id,
        count(distinct p.payment_id) as total_payment_attempts,
        count(distinct case when p.status = 'FAILED' then p.payment_id end) as failed_payments

    from orders o
    left join payments p on o.order_id = p.order_id
    group by o.customer_id

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.country,
        c.signup_date,
        c.is_active,

        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.delivered_orders, 0) as delivered_orders,
        coalesce(co.cancelled_orders, 0) as cancelled_orders,
        coalesce(co.lifetime_revenue, 0) as lifetime_revenue,
        co.first_order_date,
        co.most_recent_order_date,

        coalesce(cp.total_payment_attempts, 0) as total_payment_attempts,
        coalesce(cp.failed_payments, 0) as failed_payments,

        case
            when co.total_orders > 0
            then round(co.lifetime_revenue / co.total_orders, 2)
            else 0
        end as avg_order_value,

        case
            when co.most_recent_order_date is not null
            then datediff(current_date(), co.most_recent_order_date)
            else null
        end as days_since_last_order

    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
    left join customer_payment_health cp on c.customer_id = cp.customer_id

)

select * from final
