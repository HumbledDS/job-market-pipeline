import sqlite3
import pandas as pd
import logging
import json
import glob
from typing import List, Dict
from pathlib import Path
from config.settings import Settings

class SQLiteLoader:
    """
    Loads data into SQLite database
    
    Educational Notes:
    - SQLite is lightweight, serverless database
    - Perfect for local development and learning
    - ACID compliant with excellent SQL support
    - No compilation issues on Windows
    """
    
    def __init__(self):
        self.settings = Settings()
        self.db_path = Path(self.settings.DATABASE_PATH)
        self.logger = logging.getLogger(__name__)
        
        # Ensure data directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._initialize_database()
    
    def find_latest_data_file(self) -> str:
        """Find the most recent comprehensive job data file"""
        pattern = "data/comprehensive_job_data_*.json"
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError("No comprehensive job data files found")
        
        # Sort by filename (timestamp) to get the latest
        latest_file = sorted(files)[-1]
        self.logger.info(f"Using latest data file: {latest_file}")
        return latest_file
    
    def load_latest_data(self) -> Dict:
        """Load the most recent comprehensive job data"""
        latest_file = self.find_latest_data_file()
        
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        self.logger.info(f"Loaded data from {latest_file}")
        return data
    
    def _initialize_database(self):
        """Create database schema"""
        with sqlite3.connect(str(self.db_path)) as conn:
            # Raw jobs table - stores data in normalized form
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_jobs (
                    id VARCHAR PRIMARY KEY,
                    title VARCHAR,
                    company VARCHAR,
                    location VARCHAR,
                    salary_min REAL,
                    salary_max REAL,
                    description TEXT,
                    contract_type VARCHAR,
                    category VARCHAR,
                    created TIMESTAMP,
                    redirect_url VARCHAR,
                    search_location VARCHAR,
                    search_keyword VARCHAR,
                    extracted_at TIMESTAMP,
                    raw_data TEXT  -- Store JSON as text in SQLite
                )
            """)
            
            # Companies dimension table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dim_companies (
                    company_name VARCHAR PRIMARY KEY,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    total_jobs INTEGER
                )
            """)
            
            # Locations dimension table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dim_locations (
                    location VARCHAR PRIMARY KEY,
                    city VARCHAR,
                    state VARCHAR,
                    country VARCHAR,
                    coordinates VARCHAR
                )
            """)
            
            self.logger.info("SQLite database schema initialized")
    
    def clear_database(self):
        """Clear all data from database"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("DELETE FROM raw_jobs")
            self.logger.info("Cleared existing data from database")

    def load_raw_jobs(self, jobs: List[Dict]) -> int:
        """Load raw job data into database"""
        if not jobs:
            return 0
        
        # Convert to DataFrame for easier handling
        df = pd.DataFrame(jobs)
        
        # Clean and prepare data
        df = self._clean_job_data(df)
        
        with sqlite3.connect(str(self.db_path)) as conn:
            # FIX: Use INSERT OR REPLACE to handle duplicates
            chunk_size = 100
            total_inserted = 0
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                
                # Use INSERT OR REPLACE strategy
                chunk.to_sql('raw_jobs_temp', conn, if_exists='replace', index=False)
                
                # Insert with conflict resolution
                conn.execute("""
                    INSERT OR REPLACE INTO raw_jobs 
                    SELECT * FROM raw_jobs_temp
                """)
                
                total_inserted += len(chunk)
                
                if i % 500 == 0:
                    self.logger.info(f"Processed {total_inserted} jobs so far...")
            
            # Clean up temp table
            conn.execute("DROP TABLE IF EXISTS raw_jobs_temp")
            
            self.logger.info(f"Finished processing {total_inserted} jobs into raw_jobs table")
            return total_inserted
    
    def _clean_job_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize job data"""
        # Map field names from extracted data to database schema
        field_mapping = {
            'company_name': 'company',  # Map extracted field to DB field
            'location_display': 'location',
            'category_label': 'category'
        }
        
        # Apply field mapping
        for extracted_field, db_field in field_mapping.items():
            if extracted_field in df.columns:
                df[db_field] = df[extracted_field]
        
        # Extract salary information
        df['salary_min'] = df.get('salary_min', 0).fillna(0)
        df['salary_max'] = df.get('salary_max', 0).fillna(0)
        
        # Parse dates
        df['created'] = pd.to_datetime(df.get('created'), errors='coerce')
        df['extracted_at'] = pd.to_datetime(df.get('extracted_at'))
        
        # Clean text fields
        df['title'] = df.get('title', 'Unknown').fillna('Unknown')
        df['company'] = df.get('company', 'Unknown').fillna('Unknown')
        df['description'] = df.get('description', '').fillna('')
        
        # Store original data as JSON string
        df['raw_data'] = df.to_json(orient='records')
        
        # Select only the columns we need
        columns = [
            'id', 'title', 'company', 'location', 'salary_min', 'salary_max',
            'description', 'contract_type', 'category', 'created', 'redirect_url',
            'search_location', 'search_keyword', 'extracted_at', 'raw_data'
        ]
        
        # Only keep columns that exist in the DataFrame
        available_columns = [col for col in columns if col in df.columns]
        
        return df[available_columns]
    
    def get_job_stats(self) -> Dict:
        """Get basic statistics about loaded data"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(DISTINCT company) as unique_companies,
                    COUNT(DISTINCT location) as unique_locations,
                    AVG(salary_max) as avg_max_salary,
                    MIN(created) as earliest_job,
                    MAX(created) as latest_job
                FROM raw_jobs
                WHERE salary_max > 0
            """)
            
            result = cursor.fetchone()
            
            return {
                'total_jobs': result[0] if result[0] else 0,
                'unique_companies': result[1] if result[1] else 0,
                'unique_locations': result[2] if result[2] else 0,
                'avg_max_salary': result[3] if result[3] else 0,
                'earliest_job': result[4] if result[4] else None,
                'latest_job': result[5] if result[5] else None
            }
        
    def add_transformation_columns(self):
        """Add columns for transformed data"""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute("ALTER TABLE raw_jobs ADD COLUMN skills_extracted TEXT")
                conn.execute("ALTER TABLE raw_jobs ADD COLUMN seniority_level VARCHAR")
                conn.execute("ALTER TABLE raw_jobs ADD COLUMN is_remote BOOLEAN")
                conn.execute("ALTER TABLE raw_jobs ADD COLUMN location_city VARCHAR")
                conn.execute("ALTER TABLE raw_jobs ADD COLUMN location_state VARCHAR")
                conn.execute("ALTER TABLE raw_jobs ADD COLUMN location_country VARCHAR")
                self.logger.info("Added transformation columns to database")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    self.logger.info("Transformation columns already exist")
                else:
                    raise

# Test the loader
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Initialize loader
    loader = SQLiteLoader()
    
    # Clear existing data first
    loader.clear_database()

    # Load latest comprehensive data
    data = loader.load_latest_data()
    
    # Load jobs into database
    jobs_loaded = loader.load_raw_jobs(data['jobs'])
    
    # Get statistics
    stats = loader.get_job_stats()
    
    print(f"\n📊 DATABASE LOADING RESULTS:")
    print(f"Jobs loaded: {jobs_loaded}")
    print(f"Database stats: {stats}")