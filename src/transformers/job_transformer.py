import re
import logging
from typing import List, Dict, Set
import pandas as pd

class JobTransformer:
    """
    Transforms and enriches job data
    
    Educational Notes:
    - Data cleaning removes inconsistencies
    - Feature engineering creates new valuable fields
    - Normalization standardizes formats
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Skills patterns for extraction
        self.tech_skills = {
            'python', 'sql', 'java', 'javascript', 'react', 'node.js',
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform',
            'spark', 'hadoop', 'kafka', 'airflow', 'dbt', 'snowflake',
            'tableau', 'power bi', 'looker', 'git', 'jenkins', 'ci/cd'
        }
    
    def extract_skills(self, description: str) -> List[str]:
        """Extract technical skills from job description"""
        if not description:
            return []
        
        description_lower = description.lower()
        found_skills = []
        
        for skill in self.tech_skills:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(skill.lower()) + r'\b'
            if re.search(pattern, description_lower):
                found_skills.append(skill)
        
        return found_skills
    
    def classify_seniority(self, title: str, description: str) -> str:
        """Classify job seniority level"""
        title_lower = title.lower()
        desc_lower = description.lower() if description else ""
        
        # Senior level indicators
        senior_keywords = ['senior', 'lead', 'principal', 'staff', 'architect']
        if any(keyword in title_lower for keyword in senior_keywords):
            return 'Senior'
        
        # Junior level indicators
        junior_keywords = ['junior', 'entry', 'graduate', 'intern', 'associate']
        if any(keyword in title_lower for keyword in junior_keywords):
            return 'Junior'
        
        # Check years of experience in description
        exp_pattern = r'(\d+)\+?\s*years?\s*of?\s*experience'
        matches = re.findall(exp_pattern, desc_lower)
        if matches:
            years = int(matches[0])
            if years >= 5:
                return 'Senior'
            elif years <= 2:
                return 'Junior'
        
        return 'Mid'
    
    def normalize_salary(self, salary_min: float, salary_max: float) -> Dict:
        """Normalize salary information"""
        # Handle missing or invalid salaries
        if not salary_min or salary_min <= 0:
            salary_min = None
        if not salary_max or salary_max <= 0:
            salary_max = None
        
        # Calculate annual salary if needed
        annual_min = salary_min
        annual_max = salary_max
        
        # If values seem like hourly rates, convert to annual
        if salary_max and salary_max < 200:  # Likely hourly
            annual_min = salary_min * 40 * 52 if salary_min else None
            annual_max = salary_max * 40 * 52 if salary_max else None
        
        return {
            'salary_min_annual': annual_min,
            'salary_max_annual': annual_max,
            'salary_midpoint': (annual_min + annual_max) / 2 if annual_min and annual_max else None
        }
    
    def extract_location_details(self, location: str) -> Dict:
        """Parse location into components"""
        if not location:
            return {'city': None, 'state': None, 'country': None}
        
        # Simple parsing - can be enhanced with geocoding APIs
        parts = [part.strip() for part in location.split(',')]
        
        if len(parts) >= 2:
            city = parts[0]
            state = parts[1] if len(parts) > 2 else parts[1]
            country = parts[-1] if len(parts) > 2 else 'US'
        else:
            city = parts[0]
            state = None
            country = 'FR'
        
        return {
            'city': city,
            'state': state,
            'country': country
        }
    
    def is_remote_job(self, title: str, description: str, location: str) -> bool:
        """Determine if job is remote"""
        text = f"{title} {description} {location}".lower()
        remote_indicators = ['remote', 'work from home', 'telecommute', 'distributed', 'télétravail']
        
        return any(indicator in text for indicator in remote_indicators)


    def apply_transformations_to_database():
        """Apply transformations to all jobs in the database"""
        import sqlite3
        from config.settings import Settings
        
        settings = Settings()
        transformer = JobTransformer()
        
        # Connect to database
        with sqlite3.connect(settings.DATABASE_PATH) as conn:
            # Get all jobs from database
            cursor = conn.cursor()
            cursor.execute("SELECT id, title, description, location FROM raw_jobs")
            jobs = cursor.fetchall()
            
            print(f"Applying transformations to {len(jobs)} jobs...")
            
            # Process each job
            for i, (job_id, title, description, location) in enumerate(jobs):
                if i % 500 == 0:
                    print(f"Processed {i} jobs...")
                
                # Apply transformations
                skills = transformer.extract_skills(description or "")
                seniority = transformer.classify_seniority(title or "", description or "")
                is_remote = transformer.is_remote_job(title or "", description or "", location or "")
                location_details = transformer.extract_location_details(location or "")
                
                # Update database with transformed data
                cursor.execute("""
                    UPDATE raw_jobs 
                    SET 
                        skills_extracted = ?,
                        seniority_level = ?,
                        is_remote = ?,
                        location_city = ?,
                        location_state = ?,
                        location_country = ?
                    WHERE id = ?
                """, (
                    ','.join(skills),  # Store as comma-separated string
                    seniority,
                    is_remote,
                    location_details['city'],
                    location_details['state'], 
                    location_details['country'],
                    job_id
                ))
            
            conn.commit()
            print(f"✅ Applied transformations to {len(jobs)} jobs")

if __name__ == "__main__":
    # Existing test code...
    transformer = JobTransformer()
    description = "We need a Python developer with SQL and AWS experience"
    skills = transformer.extract_skills(description)
    print(f"Skills found: {skills}")
    
    


""" # Test transformer
if __name__ == "__main__":
    transformer = JobTransformer()
    
    # Test skill extraction
    description = "We need a Python developer with SQL and AWS experience"
    skills = transformer.extract_skills(description)
    print(f"Skills found: {skills}")  """