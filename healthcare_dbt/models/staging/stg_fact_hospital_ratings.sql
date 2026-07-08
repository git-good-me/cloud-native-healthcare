/*
  stg_fact_hospital_ratings
  =========================
  Staging model over the Gold fact_hospital_ratings table.
  Casts rating to integer, keeps snapshot history intact.

  Source: s3://cloud-native-healthcare-gold/gold/pyspark/fact_hospital_ratings/
  Grain:  one row per hospital_id x snapshot_date
*/

with source as (

    select * from {{ source('gold', 'fact_hospital_ratings') }}

),

cleaned as (

    select
        cast(hospital_id              as varchar)  as hospital_id,
        cast(hospital_name            as varchar)  as hospital_name,
        cast(upper(state)             as varchar(2)) as state,
        cast(hospital_type            as varchar)  as hospital_type,
        cast(snapshot_date            as varchar)  as snapshot_date,

        -- Core rating — cast to int, null if not a valid 1-5
        case
            when try_cast(hospital_overall_rating as integer) between 1 and 5
            then cast(hospital_overall_rating as integer)
            else null
        end                                         as star_rating,

        cast(rating_eligible          as boolean)  as rating_eligible,
        cast(has_overall_rating       as boolean)  as has_overall_rating,

        -- Performance percentages (already floats in Gold)
        cast(mort_better_pct          as double)   as mort_better_pct,
        cast(safety_better_pct        as double)   as safety_better_pct,
        cast(readm_better_pct         as double)   as readm_better_pct

    from source

    where hospital_id is not null

)

select * from cleaned