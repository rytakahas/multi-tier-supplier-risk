select
  shipment_id,
  date(ship_date) as ship_date,
  supplier_id,
  part_id,
  facility_id,
  cast(qty as int64) as qty,
  cast(lead_time_days as int64) as lead_time_days,
  status
from {{ source('raw', 'shipments') }}
