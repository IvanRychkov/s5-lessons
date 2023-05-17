delete
from dds.dm_restaurants
where true;
alter sequence dds.dm_restaurants_id_seq restart with 1;
with jsoned as (select (object_value ::json #>> '{}')::json as j
                from stg.ordersystem_restaurants)
insert
into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
select j ->> '_id',
       j ->> 'name',
       (j ->> 'update_ts')::timestamp,
       '2099-12-31 00:00:00.000'
from jsoned;
