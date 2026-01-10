select
  supplier_id,
  supplier_name,
  cast(tier as int64) as tier,
  country_code
from {{ source('raw', 'suppliers') }}
