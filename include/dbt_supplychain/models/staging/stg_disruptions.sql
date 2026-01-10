select
  disruption_id,
  supplier_id,
  date(start_date) as start_date,
  date(end_date) as end_date,
  disruption_type,
  cast(severity as float64) as severity
from {{ source('raw', 'disruptions') }}
