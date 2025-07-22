import sqlite3
from config.settings import Settings

settings = Settings()

# SQL to create views from existing data
sql_commands = """
CREATE VIEW IF NOT EXISTS stg_jobs AS
SELECT 
    id, title, company, location,
    CAST(salary_min AS REAL) as salary_min,
    CAST(salary_max AS REAL) as salary_max,
    seniority_level, skills_extracted, is_remote,
    location_city, location_state, location_country,
    search_keyword, search_location,
    DATE(created) as job_posted_date,
    DATE(extracted_at) as data_extracted_date
FROM raw_jobs
WHERE salary_max > 1000;

CREATE VIEW IF NOT EXISTS dim_companies AS
SELECT 
    company,
    COUNT(*) as total_jobs_posted,
    AVG(salary_max) as avg_max_salary,
    MIN(DATE(created)) as first_job_posted,
    MAX(DATE(created)) as last_job_posted
FROM stg_jobs 
GROUP BY company
HAVING COUNT(*) >= 2;

CREATE VIEW IF NOT EXISTS skills_analysis AS
SELECT 
    TRIM(skill_name) as skill_name,
    COUNT(*) as job_count,
    AVG(salary_max) as avg_salary,
    seniority_level
FROM (
    SELECT 
        CASE 
            WHEN skills_extracted LIKE '%python%' THEN 'python'
            WHEN skills_extracted LIKE '%sql%' THEN 'sql'  
            WHEN skills_extracted LIKE '%aws%' THEN 'aws'
            WHEN skills_extracted LIKE '%java%' THEN 'java'
            WHEN skills_extracted LIKE '%react%' THEN 'react'
        END as skill_name,
        salary_max, seniority_level
    FROM stg_jobs
    WHERE skills_extracted IS NOT NULL
) skills
WHERE skill_name IS NOT NULL
GROUP BY skill_name, seniority_level;
"""

with sqlite3.connect(settings.DATABASE_PATH) as conn:
    for statement in sql_commands.split(';'):
        statement = statement.strip()
        if statement:
            conn.execute(statement)
    conn.commit()

print("✅ Analytics views created successfully!")

# Test the views
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM stg_jobs")
print(f"Staging jobs: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM dim_companies")
print(f"Companies: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM skills_analysis")
print(f"Skills: {cursor.fetchone()[0]}")
