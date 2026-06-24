-- ============================================================
-- Cloud-Native Healthcare Analytics Queries
-- Target: AWS Athena | Database: healthcare_lakehouse
-- ============================================================

-- Q1: Hospital Performance Distribution
-- Shows breakdown of US hospitals by performance tier
SELECT 
    performance_tier,
    COUNT(*) as hospital_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM healthcare_lakehouse.gold_hospital_scorecard
GROUP BY performance_tier
ORDER BY hospital_count DESC;

-- Q2: Top 10 States by Average Hospital Rating
-- Identifies which states have the highest quality hospitals
SELECT 
    state,
    COUNT(*) as total_hospitals,
    ROUND(AVG(hospital_overall_rating), 2) as avg_rating,
    SUM(CASE WHEN performance_tier = 'High Performing' THEN 1 ELSE 0 END) as high_performing_count
FROM healthcare_lakehouse.gold_hospital_scorecard
WHERE hospital_overall_rating IS NOT NULL
GROUP BY state
ORDER BY avg_rating DESC
LIMIT 10;

-- Q3: Hospitals with Worst Readmission and Infection Scores
-- Identifies highest-risk hospitals for patient safety
SELECT 
    s.hospital_name,
    s.state,
    s.hospital_type,
    s.hospital_overall_rating,
    s.readmission_avg_score,
    s.infections_avg_score,
    s.performance_tier
FROM healthcare_lakehouse.gold_hospital_scorecard s
WHERE s.readmission_avg_score IS NOT NULL
    AND s.infections_avg_score IS NOT NULL
    AND s.hospital_overall_rating IS NOT NULL
ORDER BY s.readmission_avg_score DESC
LIMIT 10;

-- Q4: Patient Satisfaction vs Hospital Rating Correlation
-- Validates that star ratings align with patient experience
SELECT 
    hospital_overall_rating,
    ROUND(AVG(overall_hcahps_star_rating), 2) as avg_satisfaction_stars,
    ROUND(AVG(total_surveys_completed), 0) as avg_surveys,
    COUNT(*) as hospital_count
FROM healthcare_lakehouse.gold_hospital_scorecard
WHERE hospital_overall_rating IS NOT NULL
    AND overall_hcahps_star_rating IS NOT NULL
GROUP BY hospital_overall_rating
ORDER BY hospital_overall_rating;

-- Q5: Hospital Rating Trends Over Time (2024-2026)
-- Tracks how US hospital quality has changed across 9 snapshots
SELECT 
    snapshot_date,
    ROUND(AVG(hospital_overall_rating), 3) as avg_rating,
    COUNT(*) as hospitals_rated,
    SUM(CASE WHEN hospital_overall_rating >= 4 THEN 1 ELSE 0 END) as high_performers
FROM healthcare_lakehouse.fact_hospital_ratings
WHERE hospital_overall_rating IS NOT NULL
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- Q6: Hospital Ownership vs Performance
-- Compares government, non-profit, and for-profit hospital quality
SELECT 
    hospital_ownership,
    COUNT(*) as hospital_count,
    ROUND(AVG(hospital_overall_rating), 2) as avg_rating,
    SUM(CASE WHEN performance_tier = 'High Performing' THEN 1 ELSE 0 END) as high_performing,
    SUM(CASE WHEN performance_tier = 'Below Average' THEN 1 ELSE 0 END) as below_average
FROM healthcare_lakehouse.gold_hospital_scorecard
WHERE hospital_overall_rating IS NOT NULL
GROUP BY hospital_ownership
ORDER BY avg_rating DESC;

-- Q7: Emergency Services Impact on Rating
-- Do hospitals with emergency services perform better?
SELECT 
    emergency_services,
    COUNT(*) as hospital_count,
    ROUND(AVG(hospital_overall_rating), 2) as avg_rating,
    ROUND(AVG(overall_hcahps_star_rating), 2) as avg_satisfaction
FROM healthcare_lakehouse.gold_hospital_scorecard
WHERE hospital_overall_rating IS NOT NULL
GROUP BY emergency_services
ORDER BY avg_rating DESC;