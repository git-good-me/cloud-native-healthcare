/*
  assert_star_rating_valid_range
  ==============================
  Custom test: all non-null star ratings in the scorecard staging model
  must fall between 1 and 5 inclusive.

  Returns rows that FAIL the test (dbt expects 0 rows for a passing test).
*/

select
    hospital_id,
    hospital_name,
    star_rating
from {{ ref('stg_hospital_scorecard') }}
where star_rating is not null
  and (star_rating < 1 or star_rating > 5)