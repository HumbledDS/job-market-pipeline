-- Staging view: Clean jobs data
CREATE VIEW IF NOT EXISTS stg_jobs AS
SELECT 
    id,
    title,
    company,
    location,
    CAST(salary_min AS REAL) as salary_min,
    CAST(salary_max AS REAL) as salary_max,
    seniority_level,
    skills_extracted,
    is_remote,
    location_city,
    location_state,
    location_country,
    search_keyword,
    search_location,
    DATE(created) as job_posted_date,
    DATE(extracted_at) as data_extracted_date
FROM raw_jobs
WHERE salary_max > 1000;  -- Filter out bad data

-- Companies dimension
CREATE VIEW IF NOT EXISTS dim_companies AS
SELECT 
    company,
    COUNT(*) as total_jobs_posted,
    AVG(salary_max) as avg_max_salary,
    MIN(DATE(created)) as first_job_posted,
    MAX(DATE(created)) as last_job_posted
FROM stg_jobs 
GROUP BY company
HAVING COUNT(*) >= 2;  -- Companies with at least 2 jobs

-- Skills analysis
CREATE VIEW IF NOT EXISTS skills_analysis AS
SELECT 
    TRIM(skill.value) as skill_name,
    COUNT(*) as job_count,
    AVG(salary_max) as avg_salary,
    seniority_level
FROM stg_jobs,
     json_each('["' || REPLACE(skills_extracted, ',', '","') || '"]') skill
WHERE skills_extracted IS NOT NULL 
  AND skills_extracted != ''
GROUP BY TRIM(skill.value), seniority_level
ORDER BY job_count DESC;

-- Job market fact table
CREATE VIEW IF NOT EXISTS fact_job_market AS
SELECT 
    job_posted_date,
    search_keyword,
    location_city,
    location_state,
    seniority_level,
    is_remote,
    COUNT(*) as jobs_posted,
    AVG(salary_max) as avg_max_salary,
    MIN(salary_max) as min_salary,
    MAX(salary_max) as max_salary
FROM stg_jobs
GROUP BY 1,2,3,4,5,6;