select
  supplier_id,
  part_id
from {{ source('raw', 'supplier_parts') }}
