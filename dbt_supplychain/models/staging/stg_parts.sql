select
  part_id,
  part_name,
  criticality
from {{ source('raw', 'parts') }}
