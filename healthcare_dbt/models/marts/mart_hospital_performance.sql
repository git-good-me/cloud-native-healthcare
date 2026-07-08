/*
  mart_hospital_performance
  =========================
  Business-level hospital performance metrics aggregated by state.
  Primary use: BI dashboards, Athena ad-hoc queries.

  Metrics defined:
    - total_hospitals           : count of unique hospitals per state
    - avg_star_rating           : average CMS star rating (1-5)
    - pct_above_avg_rating      : % of hospitals rated 4 or 5 stars
    - avg_readmission_rate      : average readmission score (lower = better)
    - avg_complication_score    : average complication score
    - avg_infection_score       : average infection score
    - avg_hcahps_satisfaction   : average patient satisfaction rating
    - top_performer_count       : hospitals in "High" performance tier
    - low_performer_count       : hospitals in "Low" performance tier

  Grain: one row per state
*/

with scorecard as (

    select * from {{ ref('stg_hospital_scorecard') }}

),

state_metrics as (

    select
        state,

        -- Volume
        count(distinct hospital_id)                         as total_hospitals,

        -- Star ratings
        round(avg(cast(star_rating as double)), 2)          as avg_star_rating,
        round(
            100.0 * sum(case when star_rating >= 4 then 1 else 0 end)
            / nullif(count(star_rating), 0),
        1)                                                  as pct_high_rated,

        -- Quality metrics (lower = better for most)
        round(avg(readmission_avg_score), 3)                as avg_readmission_score,
        round(avg(complications_avg_score), 3)              as avg_complication_score,
        round(avg(infections_avg_score), 3)                 as avg_infection_score,

        -- Patient satisfaction
        round(avg(avg_satisfaction_rating), 2)              as avg_satisfaction_rating,
        round(avg(hcahps_star_rating), 2)                   as avg_hcahps_star_rating,

        -- Survey volume
        sum(total_surveys_completed)                        as total_surveys_completed,

        -- Performance tiers
        sum(case when upper(performance_tier) = 'HIGH'   then 1 else 0 end) as top_performer_count,
        sum(case when upper(performance_tier) = 'MEDIUM' then 1 else 0 end) as mid_performer_count,
        sum(case when upper(performance_tier) = 'LOW'    then 1 else 0 end) as low_performer_count,

        -- Medicare efficiency
        round(avg(medicare_spending_score), 3)              as avg_medicare_spending_score

    from scorecard
    where state is not null
    group by state

),

ranked as (

    select
        *,
        rank() over (order by avg_star_rating desc nulls last)      as star_rating_rank,
        rank() over (order by avg_readmission_score asc nulls last)  as readmission_rank
    from state_metrics

)

select * from ranked
order by avg_star_rating desc nulls last