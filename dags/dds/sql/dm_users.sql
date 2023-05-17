truncate table dds.dm_users cascade;
alter sequence dds.dm_users_id_seq restart with 1;
with jsoned as (select (object_value ::json #>> '{}')::json as j
                from stg.ordersystem_users)
insert
into dds.dm_users (user_id, user_name, user_login)
select j ->> '_id',
       j ->> 'name',
       j ->> 'login'
from jsoned;
