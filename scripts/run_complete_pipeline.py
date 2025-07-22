#!/usr/bin/env python3
"""
Complete pipeline test script
Runs the entire data pipeline end-to-end
"""

import logging
import sys
import os
import sqlite3
from pathlib import Path
from typing import Dict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.extractors.adzuna_extractor import AdzunaExtractor
from src.loaders.sqlite_loader import SQLiteLoader
from src.transformers.job_transformer import JobTransformer

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/pipeline.log'),
            logging.StreamHandler()
        ]
    )

def create_analytics_views():
    """Create analytics views in SQLite"""
    from config.settings import Settings
    settings = Settings()
    
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
                WHEN skills_extracted LIKE '%javascript%' THEN 'javascript'
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

def apply_transformations():
    """Apply job transformations to database"""
    from config.settings import Settings
    settings = Settings()
    transformer = JobTransformer()
    
    with sqlite3.connect(settings.DATABASE_PATH) as conn:
        # Add transformation columns if they don't exist
        try:
            conn.execute("ALTER TABLE raw_jobs ADD COLUMN skills_extracted TEXT")
            conn.execute("ALTER TABLE raw_jobs ADD COLUMN seniority_level VARCHAR")
            conn.execute("ALTER TABLE raw_jobs ADD COLUMN is_remote BOOLEAN")
            conn.execute("ALTER TABLE raw_jobs ADD COLUMN location_city VARCHAR")
            conn.execute("ALTER TABLE raw_jobs ADD COLUMN location_state VARCHAR")
            conn.execute("ALTER TABLE raw_jobs ADD COLUMN location_country VARCHAR")
        except sqlite3.OperationalError:
            pass  # Columns already exist
        
        # Get jobs that need transformation
        cursor = conn.cursor()
        cursor.execute("SELECT id, title, description, location FROM raw_jobs WHERE skills_extracted IS NULL LIMIT 1000")
        jobs = cursor.fetchall()
        
        print(f"Applying transformations to {len(jobs)} jobs...")
        
        for i, (job_id, title, description, location) in enumerate(jobs):
            if i % 100 == 0:
                print(f"Processed {i} jobs...")
            
            # Apply transformations
            skills = transformer.extract_skills(description or "")
            seniority = transformer.classify_seniority(title or "", description or "")
            is_remote = transformer.is_remote_job(title or "", description or "", location or "")
            
            # Simple location parsing
            location_parts = location.split(',') if location else []
            city = location_parts[0].strip() if location_parts else None
            state = location_parts[1].strip() if len(location_parts) > 1 else None
            country = location_parts[-1].strip() if len(location_parts) > 2 else 'US'
            
            # Update database
            cursor.execute("""
                UPDATE raw_jobs 
                SET skills_extracted = ?, seniority_level = ?, is_remote = ?,
                    location_city = ?, location_state = ?, location_country = ?
                WHERE id = ?
            """, (','.join(skills), seniority, is_remote, city, state, country, job_id))
        
        conn.commit()
        print(f"✅ Applied transformations to {len(jobs)} jobs")

def run_complete_pipeline():
    """Run the complete data pipeline"""
    logger = logging.getLogger(__name__)
    logger.info("Starting complete pipeline run")
    
    try:
        # Step 1: Extract data
        logger.info("Step 1: Extracting job data from API")
        extractor = AdzunaExtractor()
        
        all_jobs = []
        searches = [
            {"what": "data engineer", "location": "paris", "max_pages": 3},
            {"what": "data scientist", "location": "london", "max_pages": 2},
        ]
        
        for search in searches:
            jobs = extractor.extract_jobs(**search)
            all_jobs.extend(jobs)
            logger.info(f"Extracted {len(jobs)} jobs for {search['what']} in {search['location']}")
        
        logger.info(f"Total jobs extracted: {len(all_jobs)}")
        
        # Step 2: Load data
        logger.info("Step 2: Loading data into SQLite")
        loader = SQLiteLoader()
        loader.clear_database()  # Clear existing data
        rows_inserted = loader.load_raw_jobs(all_jobs)
        logger.info(f"Inserted {rows_inserted} rows")
        
        # Step 3: Apply transformations
        logger.info("Step 3: Applying data transformations")
        apply_transformations()
        
        # Step 4: Create analytics views
        logger.info("Step 4: Creating analytics views")
        create_analytics_views()
        
        # Step 5: Generate statistics
        logger.info("Step 5: Generating pipeline statistics")
        stats = loader.get_job_stats()
        
        logger.info("Pipeline completed successfully!")
        logger.info(f"Final statistics: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    setup_logging()
    success = run_complete_pipeline()
    
    if success:
        print("\n🎉 Pipeline completed successfully!")
        print("You can now:")
        print("1. View Dashboard: streamlit run dashboard/job_market_dashboard.py")
        print("2. Check analytics views in SQLite database")
        print("3. Explore the data with: sqlite3 data/jobs.db")
    else:
        print("\n❌ Pipeline failed. Check logs for details.")
        sys.exit(1)