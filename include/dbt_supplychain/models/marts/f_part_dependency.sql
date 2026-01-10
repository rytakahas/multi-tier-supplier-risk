select
  ps.parent_part_id as parent_part_key,
  ps.child_part_id as child_part_key,
  ps.qty
from {{ ref('stg_part_subcomponents') }} ps
