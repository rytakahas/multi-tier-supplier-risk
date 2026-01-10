select
  pc.product_id as product_key,
  pc.part_id as part_key,
  pc.qty
from {{ ref('stg_product_components') }} pc
