/*
  stg_hospital_scorecard
  ======================
  Staging model over the Gold hospital scorecard.
  Normalises column names and casts all metrics to double.

  Source: s3://cloud-native-healthcare-gold/gold/pyspark/gold_hospital_scorecard/
  Grain:  one row per hospital_id
*/

with source as (

    select * from {{ source('gold', 'gold_hospital_scorecard') }}

),

cleaned as (

    select
        cast(hospital_id                as varchar)  as hospital_id,
        cast(hospital_name              as varchar)  as hospital_name,
        cast(upper(state)               as varchar(2)) as state,
        cast(hospital_type              as varchar)  as hospital_type,

        -- Rating
        case
            when try_cast(hospital_overall_rating as integer) between 1 and 5
            then cast(hospital_overall_rating as integer)
            else null
        end                                           as star_rating,

        -- Performance metrics
        cast(mort_better_pct            as double)   as mort_better_pct,
        cast(safety_better_pct          as double)   as safety_better_pct,
        cast(readm_better_pct           as double)   as readm_better_pct,
        cast(complications_avg_score    as double)   as complications_avg_score,
        cast(infections_avg_score       as double)   as infections_avg_score,
        cast(readmission_avg_score      as double)   as readmission_avg_score,
        cast(medicare_spending_score    as double)   as medicare_spending_score,
        cast(complications_worse_count  as integer)  as complications_worse_count,
        cast(readmission_worse_count as double)  as readmission_worse_count,

        -- Patient satisfaction
        cast(overall_hcahps_star_rating as double)   as hcahps_star_rating,
        cast(avg_star_rating            as double)   as avg_satisfaction_rating,
        cast(total_surveys_completed    as integer)  as total_surveys_completed,

        -- Composite
        cast(performance_tier           as varchar)  as performance_tier,
        cast(scorecard_date             as varchar)  as scorecard_date

    from source

    where hospital_id is not null

)

select * from cleaned
