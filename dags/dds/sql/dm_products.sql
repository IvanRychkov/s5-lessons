truncate table dds.dm_products cascade;
alter sequence dds.dm_products_id_seq restart with 1;
with products as (select object_id as restaurant_id, json_array_elements(object_value::json -> 'menu') as p, update_ts
                  from stg.ordersystem_restaurants)
insert
into dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
select r.id,
       p ->> '_id',
       p ->> 'name',
       (p ->> 'price')::numeric(14, 2),
       update_ts,
       timestamp '2099-12-31 00:00:00.000'
from products p
         join dds.dm_restaurants r using (restaurant_id)
order by 1, 2
