with orders as  (
    select * from {{ ref('stg_jaffle_shop__orders' )}}
),

payments as (
    select * from {{ ref('stg_stripe__payment') }}
),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount_usd end) as amount_usd

    from payments
    group by 1
),

final as (

    select
       {{ dbt_utils.generate_surrogate_key(['orders.order_id', 'orders.customer_id', 'orders.order_date']) }} as fct_order_pk,
        orders.order_id,
        orders.customer_id,
        orders.order_date,
       -- coalesce(order_payments.amount_usd, 0) as amount_usd
    coalesce(order_payments.amount_usd, 0) * 1.07 as amount_usd,
    coalesce(order_payments.amount_usd > 20, false) as big_spend_flag

    from orders
    left join order_payments using (order_id)
)

select * from final