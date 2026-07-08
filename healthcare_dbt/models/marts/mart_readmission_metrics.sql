/*
  mart_readmission_metrics
  ========================
  Hospital-level readmission and quality metrics.
  Flags hospitals with worse-than-average readmission rates.

  Metrics defined:
    - readmission_avg_score     : hospital's average readmission score
    - national_avg_readmission  : national average (window function)
    - vs_national               : above / below / at national average
    - readmission_worse_count   : count of measures worse than national rate
    - readmission_risk_flag     : TRUE if hospital is a readmission concern

  Grain: one row per hospital_id
*/

with scorecard as (

    select * from {{ ref('stg_hospital_scorecard') }}

),

dim as (

    select
        hospital_id,
        hospital_name,
        state,
        zip_code,
        county_name,
        hospital_type,
        hospital_ownership,
        emergency_services
    from {{ ref('stg_dim_hospital') }}

),

national_avg as (

    select
        avg(readmission_avg_score) as national_readmission_avg
    from scorecard
    where readmission_avg_score is not null

),

hospital_metrics as (

    select
        s.hospital_id,
        s.hospital_name,
        s.state,
        s.star_rating,
        s.performance_tier,

        -- Readmission
        s.readmission_avg_score,
        s.readmission_worse_count,
        round(n.national_readmission_avg, 3)                          as national_readmission_avg,

        case
            when s.readmission_avg_score is null then 'Unknown'
            when s.readmission_avg_score > n.national_readmission_avg then 'Worse than national'
            when s.readmission_avg_score < n.national_readmission_avg then 'Better than national'
            else 'At national average'
        end                                                            as readmission_vs_national,

        -- Flag hospitals with elevated readmission concern
        case
            when s.readmission_avg_score > n.national_readmission_avg
              or s.readmission_worse_count > 2
            then true
            else false
        end                                                            as readmission_risk_flag,

        -- Other quality metrics for context
        s.complications_avg_score,
        s.infections_avg_score,
        s.medicare_spending_score,
        s.avg_satisfaction_rating,
        s.total_surveys_completed,
        s.scorecard_date

    from scorecard s
    cross join national_avg n

),

final as (

    select
        h.*,
        d.zip_code,
        d.county_name,
        d.hospital_type,
        d.hospital_ownership,
        d.emergency_services
    from hospital_metrics h
    left join dim d using (hospital_id)

)

select * from final
order by readmission_avg_score desc nulls last