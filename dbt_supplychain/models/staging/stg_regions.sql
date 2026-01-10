select
  region_id,
  region_name,
  country_code
from {{ source('raw', 'regions') }}
