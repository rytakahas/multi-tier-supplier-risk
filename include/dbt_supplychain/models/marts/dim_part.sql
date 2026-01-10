select
  part_id as part_key,
  part_name,
  criticality
from {{ ref('stg_parts') }}
