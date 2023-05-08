insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count,
                                      orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
                                      order_processing_fee, restaurant_reward_sum)
select o.restaurant_id,
       r.restaurant_name,
       ts.date,
       count(distinct order_id),
       sum(f.total_sum),
       sum(f.bonus_payment),
       sum(f.bonus_grant),
       sum(f.total_sum) * .25,
       sum(f.total_sum) * .75 - sum(f.bonus_payment)
from dds.fct_product_sales f
         join dds.dm_orders o on f.order_id = o.id
         join dds.dm_restaurants r on o.restaurant_id = r.id
         join dds.dm_timestamps ts on o.timestamp_id = ts.id
where o.order_status = 'CLOSED'
group by o.restaurant_id,
         r.restaurant_name,
         ts.date
on conflict (restaurant_id, settlement_date) do update
    set orders_count             = excluded.orders_count,
        orders_total_sum         = excluded.orders_total_sum,
        orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
        orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
        order_processing_fee     = excluded.order_processing_fee,
        restaurant_reward_sum    = excluded.restaurant_reward_sum
;