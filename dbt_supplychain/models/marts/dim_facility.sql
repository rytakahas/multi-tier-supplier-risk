select
  f.facility_id as facility_key,
  f.facility_name,
  f.facility_type,
  r.region_id as region_key
from {{ ref('stg_facilities') }} f
join {{ ref('stg_regions') }} r
  on f.region_id = r.region_id
