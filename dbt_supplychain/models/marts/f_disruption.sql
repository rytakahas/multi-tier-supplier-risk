select
  d.disruption_id,
  d.supplier_id as supplier_key,
  d.start_date,
  d.end_date,
  d.disruption_type,
  d.severity
from {{ ref('stg_disruptions') }} d
