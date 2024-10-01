with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

)

select * from customers
