/*
  assert_ratings_have_dim_match
  ==============================
  Custom test: every hospital_id in stg_fact_hospital_ratings
  must have a matching record in stg_dim_hospital.

  Orphaned rating records (no dim match) indicate a pipeline join issue.
  Returns rows that FAIL — dbt expects 0 rows for a passing test.
*/

select
    r.hospital_id,
    r.hospital_name,
    r.snapshot_date
from {{ ref('stg_fact_hospital_ratings') }} r
left join {{ ref('stg_dim_hospital') }} d
    on r.hospital_id = d.hospital_id
where d.hospital_id is null