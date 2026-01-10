select
  facility_id,
  facility_name,
  facility_type,
  region_id
from {{ source('raw', 'facilities') }}
