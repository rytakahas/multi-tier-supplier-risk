select
  supplier_id as supplier_key,
  supplier_name,
  tier,
  country_code
from {{ ref('stg_suppliers') }}
