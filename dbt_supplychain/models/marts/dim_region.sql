select
  region_id as region_key,
  region_name,
  country_code
from {{ ref('stg_regions') }}
