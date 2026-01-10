select
  s.shipment_id,
  s.ship_date,
  s.supplier_id as supplier_key,
  s.part_id as part_key,
  s.facility_id as facility_key,
  s.qty,
  s.lead_time_days,
  s.status
from {{ ref('stg_shipments') }} s
