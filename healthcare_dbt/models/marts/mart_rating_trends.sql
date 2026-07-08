/*
  mart_rating_trends
  ==================
  Hospital star rating trends across all 9 quarterly snapshots.
  Used to track improvement or decline over time.

  Metrics defined:
    - star_rating           : CMS rating at each snapshot
    - prev_rating           : rating at previous snapshot (LAG)
    - rating_change         : delta vs previous snapshot
    - trend                 : Improving / Declining / Stable / New
    - snapshots_above_avg   : how many snapshots rated >= 4

  Grain: one row per hospital_id x snapshot_date
*/

with ratings as (

    select * from {{ ref('stg_fact_hospital_ratings') }}

),

with_lag as (

    select
        hospital_id,
        hospital_name,
        state,
        snapshot_date,
        star_rating,

        lag(star_rating) over (
            partition by hospital_id
            order by snapshot_date
        )                                               as prev_rating,

        row_number() over (
            partition by hospital_id
            order by snapshot_date
        )                                               as snapshot_number,

        count(*) over (
            partition by hospital_id
        )                                               as total_snapshots,

        mort_better_pct,
        safety_better_pct,
        readm_better_pct

    from ratings
    where star_rating is not null

),

with_trend as (

    select
        *,

        -- Rating delta vs prior snapshot
        star_rating - prev_rating                       as rating_change,

        -- Trend label
        case
            when prev_rating is null              then 'First snapshot'
            when star_rating > prev_rating        then 'Improving'
            when star_rating < prev_rating        then 'Declining'
            else                                       'Stable'
        end                                             as trend,

        -- Count snapshots at or above 4 stars
        sum(case when star_rating >= 4 then 1 else 0 end) over (
            partition by hospital_id
        )                                               as snapshots_above_avg

    from with_lag

)

select * from with_trend
order by hospital_id, snapshot_date