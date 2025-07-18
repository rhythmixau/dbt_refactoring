{% snapshot orders_snapshot %}

{{
    config(
        target_database='general-rhythmix-playground',
        target_schema='dbt_refactor',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ ref('fct_customer_orders2') }}

{% endsnapshot %}