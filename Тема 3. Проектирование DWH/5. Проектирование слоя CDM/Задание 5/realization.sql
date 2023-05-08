alter table cdm.dm_settlement_report
    add unique (restaurant_id, settlement_date);