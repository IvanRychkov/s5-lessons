delete
from dds.dm_orders
where true;
alter sequence dds.dm_orders_id_seq restart with 1;
with orders as (select object_id as order_id, (object_value::json #>> '{}')::json as j, update_ts
                from stg.ordersystem_orders)
insert
into dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
select u.id,
       p.restaurant_id,
       -- rest
       ts.id,
       order_id,
       j ->> 'final_status'
from orders o
         join dds.dm_products p on o.j #>> '{order_items,0,id}' = p.product_id
         join dds.dm_timestamps ts on (o.j ->> 'date')::timestamp = ts.ts
         join dds.dm_users u on o.j -> 'user' ->> 'id' = u.user_id
