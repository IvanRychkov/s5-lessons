truncate table  cdm.dm_settlement_report;
alter sequence cdm.dm_settlement_report_id_seq restart with 1;
insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum,
                                      orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee,
                                      restaurant_reward_sum)
select r.restaurant_id,
       r.restaurant_name,
       ts.date,
       count(distinct order_id),
       sum(fct.total_sum),
       sum(fct.bonus_payment),
       sum(fct.bonus_grant),
       sum(fct.total_sum) * .25,
       sum(fct.total_sum) - sum(fct.bonus_payment) - sum(fct.total_sum) * .25
from dds.fct_product_sales fct
         join dds.dm_orders o on fct.order_id = o.id
    and o.order_status = 'CLOSED'
         join dds.dm_timestamps ts on o.timestamp_id = ts.id
         join dds.dm_restaurants r on o.restaurant_id = r.id
group by 1, 2, 3;
