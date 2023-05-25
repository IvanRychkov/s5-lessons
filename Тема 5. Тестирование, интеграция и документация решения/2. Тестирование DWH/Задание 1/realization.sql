select now() as  test_date_time,
       'test_01' test_name,
       count(*) = 0 as test_result
from public_test.dm_settlement_report_actual a
         full join public_test.dm_settlement_report_expected e
                   using (restaurant_id, settlement_month, settlement_year)
where a.id is null
   or e.id is null;