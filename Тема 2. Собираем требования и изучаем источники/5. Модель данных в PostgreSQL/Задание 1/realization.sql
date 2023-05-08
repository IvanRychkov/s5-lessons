select distinct json_array_elements(event_value::json->'product_payments')->>'product_name'
from outbox