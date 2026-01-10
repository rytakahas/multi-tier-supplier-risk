select
  product_id,
  part_id,
  cast(qty as int64) as qty
from {{ source('raw', 'product_components') }}
