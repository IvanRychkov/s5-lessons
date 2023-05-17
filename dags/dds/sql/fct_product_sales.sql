truncate table dds.fct_product_sales cascade;
alter sequence dds.fct_product_sales_id_seq restart with 1;
with bonus_events as (select event_value::json #>> '{order_id}'                             as order_id,
                             json_array_elements(event_value::json #> '{product_payments}') as pp
                      from stg.bonussystem_events
                      where event_type = 'bonus_transaction'),
     order_items as (select object_id                                                                 as order_id,
                            json_array_elements((object_value::json #>> '{}')::json -> 'order_items') as i
                     from stg.ordersystem_orders)
insert
into dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
select p.id,
       o.id,
       (i ->> 'quantity')::int                                   count,
       (i ->> 'price')::numeric(19, 5) as                        price,
       (i ->> 'quantity')::int * (i ->> 'price')::numeric(19, 5) total_sum,
       (be.pp ->> 'bonus_payment')::numeric(19, 5),
       (be.pp ->> 'bonus_grant')::numeric(19, 5)
from order_items oi
         join dds.dm_products p on i ->> 'id' = p.product_id
         join dds.dm_orders o on oi.order_id = o.order_key
         join bonus_events be on oi.order_id = be.order_id and p.product_id = be.pp ->> 'product_id'