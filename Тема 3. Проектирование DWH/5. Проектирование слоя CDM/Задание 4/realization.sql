alter table cdm.dm_settlement_report
    alter column orders_count set default 0,
    alter column orders_total_sum set default 0,
    alter column order_processing_fee set default 0,
    alter column orders_bonus_payment_sum set default 0,
    alter column orders_bonus_granted_sum set default 0,
    alter column restaurant_reward_sum set default 0;

alter table cdm.dm_settlement_report
    add check ( orders_count >= 0),
    add check ( orders_total_sum >= 0),
    add check ( order_processing_fee >= 0),
    add check ( orders_bonus_payment_sum >= 0),
    add check ( orders_bonus_granted_sum >= 0),
    add check ( restaurant_reward_sum >= 0);