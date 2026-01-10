select
  supplier_id,
  facility_id
from {{ source('raw', 'supplier_facilities') }}
