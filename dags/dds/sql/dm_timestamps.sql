truncate table dds.dm_timestamps cascade;
alter sequence dds.dm_timestamps_id_seq restart with 1;
with jsoned as (select (object_value ::json #>> '{}')::json as j
                from stg.ordersystem_orders),
     dates as (select distinct (j ->> 'date')::timestamp as ts
               from jsoned
               where j ->> 'final_status' in ('CLOSED', 'CANCELLED'))
insert
into dds.dm_timestamps (ts, year, month, day, time, date)
select ts,
       extract(year from ts),
       extract(month from ts),
       extract(day from ts),
       ts::time,
       ts::date
from dates;
