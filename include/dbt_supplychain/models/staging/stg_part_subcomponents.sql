select
  parent_part_id,
  child_part_id,
  cast(qty as int64) as qty
from {{ source('raw', 'part_subcomponents') }}
