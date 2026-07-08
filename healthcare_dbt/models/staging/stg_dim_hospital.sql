with source as (
    select * from {{ source('gold', 'dim_hospital') }}
),
cleaned as (
    select
        cast(hospital_id        as varchar)  as hospital_id,
        cast(hospital_name      as varchar)  as hospital_name,
        cast(address            as varchar)  as address,
        cast(city               as varchar)  as city,
        cast(upper(state)       as varchar)  as state,
        cast(zip                as varchar)  as zip_code,
        cast(county             as varchar)  as county_name,
        cast(phone              as varchar)  as phone_number,
        cast(hospital_type      as varchar)  as hospital_type,
        cast(hospital_ownership as varchar)  as hospital_ownership,
        cast(emergency_services as varchar)  as emergency_services,
        cast(has_overall_rating as boolean)  as has_overall_rating,
        cast(rating_eligible    as boolean)  as rating_eligible
    from source
    where hospital_id is not null
      and hospital_name is not null
)
select * from cleaned
