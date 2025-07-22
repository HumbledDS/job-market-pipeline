import requests
import time
import logging
from typing import List, Dict, Optional
from config.settings import Settings
import os

class AdzunaExtractor:
    """
    Enhanced Adzuna API extractor for comprehensive job market data
    
    Educational Notes:
    - Extracts jobs, salary data, top companies, and geographic insights
    - Multiple API endpoints for different types of analysis
    - Comprehensive error handling and rate limiting
    """
    
    def __init__(self):
        self.settings = Settings()
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
    
    def extract_comprehensive_data(self, 
                                 country: str = "fr", 
                                 locations: List[str] = None,
                                 job_types: List[str] = None,
                                 max_pages: int = 5) -> Dict:
        """
        Extract comprehensive job market data including:
        - Job listings
        - Salary histograms
        - Top companies
        - Geographic job distribution
        - Categories
        """
        if locations is None:
            locations = ["new york", "san francisco", "seattle", "austin", "boston"]
        
        if job_types is None:
            job_types = ["data engineer", "data scientist", "software engineer", 
                        "product manager", "devops engineer"]
        
        comprehensive_data = {
            'jobs': [],
            'salary_histograms': [],
            'top_companies': [],
            'geographic_data': [],
            'categories': [],
            'extraction_metadata': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'country': country,
                'locations_searched': locations,
                'job_types_searched': job_types
            }
        }
        
        # 1. Extract job listings for each location and job type
        self.logger.info("Starting job listings extraction...")
        for location in locations:
            for job_type in job_types:
                jobs = self.extract_jobs(country, location, job_type, max_pages)
                comprehensive_data['jobs'].extend(jobs)
                time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        # 2. Extract salary histograms for each job type
        self.logger.info("Extracting salary histograms...")
        for job_type in job_types:
            histogram = self.extract_salary_histogram(country, job_type)
            if histogram:
                comprehensive_data['salary_histograms'].append({
                    'job_type': job_type,
                    'histogram_data': histogram
                })
            time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        # 3. Extract top companies for each job type
        self.logger.info("Extracting top companies...")
        for job_type in job_types:
            companies = self.extract_top_companies(country, job_type)
            if companies:
                comprehensive_data['top_companies'].append({
                    'job_type': job_type,
                    'companies': companies
                })
            time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        # 4. Extract geographic job distribution
        self.logger.info("Extracting geographic data...")
        for job_type in job_types:
            geo_data = self.extract_geographic_data(country, job_type)
            if geo_data:
                comprehensive_data['geographic_data'].append({
                    'job_type': job_type,
                    'locations': geo_data
                })
            time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        # 5. Extract available categories
        self.logger.info("Extracting job categories...")
        categories = self.extract_categories(country)
        comprehensive_data['categories'] = categories
        
        self.logger.info(f"Comprehensive extraction complete. Total jobs: {len(comprehensive_data['jobs'])}")
        return comprehensive_data
    
    def extract_jobs(self, 
                    country: str = "fr", 
                    location: str = "paris",
                    what: str = "data engineer",
                    max_pages: int = 5) -> List[Dict]:
        """Extract job postings with enhanced data fields"""
        all_jobs = []
        
        for page in range(1, max_pages + 1):
            self.logger.info(f"Fetching jobs page {page} for '{what}' in {location}")
            
            url = f"{self.settings.ADZUNA_BASE_URL}/jobs/{country}/search/{page}"
            
            params = {
                'app_id': self.settings.ADZUNA_API_ID,
                'app_key': self.settings.ADZUNA_API_KEY,
                'results_per_page': 50,
                'what': what,
                'where': location,
                'content-type': 'application/json',
                'sort_by': 'relevance',
                'salary_include_unknown': '1'  # Include jobs without salary info
            }
            
            try:
                response = self._make_request(url, params)
                
                if response and 'results' in response:
                    jobs = response['results']
                    self.logger.info(f"Fetched {len(jobs)} jobs from page {page}")
                    
                    # Enhance each job with additional metadata
                    for job in jobs:
                        job['extracted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                        job['search_location'] = location
                        job['search_keyword'] = what
                        job['search_country'] = country
                        
                        # Extract company name safely
                        if 'company' in job and job['company']:
                            if isinstance(job['company'], dict):
                                job['company_name'] = job['company'].get('display_name', 'Unknown')
                                job['company_canonical'] = job['company'].get('canonical_name', '')
                            else:
                                job['company_name'] = str(job['company'])
                                job['company_canonical'] = ''
                        else:
                            job['company_name'] = 'Unknown'
                            job['company_canonical'] = ''
                        
                        # Extract location details
                        if 'location' in job and job['location']:
                            location_obj = job['location']
                            job['location_display'] = location_obj.get('display_name', location)
                            job['location_areas'] = location_obj.get('area', [])
                        else:
                            job['location_display'] = location
                            job['location_areas'] = []
                        
                        # Extract category information
                        if 'category' in job and job['category']:
                            category_obj = job['category']
                            job['category_label'] = category_obj.get('label', 'Unknown')
                            job['category_tag'] = category_obj.get('tag', '')
                        else:
                            job['category_label'] = 'Unknown'
                            job['category_tag'] = ''
                    
                    all_jobs.extend(jobs)
                    
                    # Check if we've reached the end
                    if len(jobs) < 50:
                        break
                else:
                    self.logger.warning(f"No results on page {page}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching jobs page {page}: {e}")
                break
            
            time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        return all_jobs
    
    def extract_salary_histogram(self, country: str, job_type: str) -> Optional[Dict]:
        """Extract salary distribution data for a job type"""
        url = f"{self.settings.ADZUNA_BASE_URL}/jobs/{country}/histogram"
        
        params = {
            'app_id': self.settings.ADZUNA_API_ID,
            'app_key': self.settings.ADZUNA_API_KEY,
            'what': job_type
        }
        
        try:
            response = self._make_request(url, params)
            if response and 'histogram' in response:
                return {
                    'job_type': job_type,
                    'histogram': response['histogram'],
                    'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S')
                }
        except Exception as e:
            self.logger.error(f"Error fetching salary histogram for {job_type}: {e}")
        
        return None
    
    def extract_top_companies(self, country: str, job_type: str) -> Optional[List[Dict]]:
        """Extract top hiring companies for a job type"""
        url = f"{self.settings.ADZUNA_BASE_URL}/jobs/{country}/top_companies"
        
        params = {
            'app_id': self.settings.ADZUNA_API_ID,
            'app_key': self.settings.ADZUNA_API_KEY,
            'what': job_type
        }
        
        try:
            response = self._make_request(url, params)
            if response and 'leaderboard' in response:
                companies = response['leaderboard']
                for company in companies:
                    company['job_type_searched'] = job_type
                    company['extracted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                return companies
        except Exception as e:
            self.logger.error(f"Error fetching top companies for {job_type}: {e}")
        
        return None
    
    def extract_geographic_data(self, country: str, job_type: str) -> Optional[List[Dict]]:
        """Extract geographic distribution of jobs"""
        url = f"{self.settings.ADZUNA_BASE_URL}/jobs/{country}/geodata"
        
        params = {
            'app_id': self.settings.ADZUNA_API_ID,
            'app_key': self.settings.ADZUNA_API_KEY,
            'what': job_type
        }
        
        try:
            response = self._make_request(url, params)
            if response and 'locations' in response:
                locations = response['locations']
                for location in locations:
                    location['job_type_searched'] = job_type
                    location['extracted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                return locations
        except Exception as e:
            self.logger.error(f"Error fetching geographic data for {job_type}: {e}")
        
        return None
    
    def extract_categories(self, country: str) -> Optional[List[Dict]]:
        """Extract all available job categories"""
        url = f"{self.settings.ADZUNA_BASE_URL}/jobs/{country}/categories"
        
        params = {
            'app_id': self.settings.ADZUNA_API_ID,
            'app_key': self.settings.ADZUNA_API_KEY
        }
        
        try:
            response = self._make_request(url, params)
            if response and 'results' in response:
                categories = response['results']
                for category in categories:
                    category['extracted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                return categories
        except Exception as e:
            self.logger.error(f"Error fetching categories: {e}")
        
        return None
    
    def extract_historical_salary_data(self, country: str, job_type: str, months: int = 12) -> Optional[Dict]:
        """Extract historical salary trends"""
        url = f"{self.settings.ADZUNA_BASE_URL}/jobs/{country}/history"
        
        params = {
            'app_id': self.settings.ADZUNA_API_ID,
            'app_key': self.settings.ADZUNA_API_KEY,
            'what': job_type,
            'months': months
        }
        
        try:
            response = self._make_request(url, params)
            if response and 'month' in response:
                return {
                    'job_type': job_type,
                    'historical_data': response['month'],
                    'months_back': months,
                    'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S')
                }
        except Exception as e:
            self.logger.error(f"Error fetching historical salary data for {job_type}: {e}")
        
        return None
    
    def _make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.settings.MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < self.settings.MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        
        return None

""" # Test the enhanced extractor
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extractor = AdzunaExtractor()
    
    # Test comprehensive extraction
    data = extractor.extract_comprehensive_data(
        country="us",
        locations=["new york", "san francisco"],
        job_types=["data engineer", "data scientist"],
        max_pages=2
    )
    
    print(f"Extracted comprehensive data:")
    print(f"- Jobs: {len(data['jobs'])}")
    print(f"- Salary histograms: {len(data['salary_histograms'])}")
    print(f"- Top companies data: {len(data['top_companies'])}")
    print(f"- Geographic data: {len(data['geographic_data'])}")
    print(f"- Categories: {len(data['categories'])}")
 """

# Save data to json

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extractor = AdzunaExtractor()
    
    # Extract comprehensive data
    data = extractor.extract_comprehensive_data(
        country="fr",
        locations=["paris", "ile de france"],
        job_types=["data engineer", "data scientist"],
        max_pages=15
    )
    
    # Print data summary
    print(f"Extracted comprehensive data:")
    print(f"- Jobs: {len(data['jobs'])}")
    print(f"- Salary histograms: {len(data['salary_histograms'])}")
    print(f"- Top companies data: {len(data['top_companies'])}")
    print(f"- Geographic data: {len(data['geographic_data'])}")
    print(f"- Categories: {len(data['categories'])}")
    
    # ========== NEW CODE TO SEE AND SAVE DATA ==========
    
    import json
    import pandas as pd
    import os
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # 1. Save raw data as JSON
    with open(f'data/comprehensive_job_data_{time.strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"\n✅ Raw data saved to: data/comprehensive_job_data_{time.strftime('%Y%m%d_%H%M%S')}.json")
    
    # 2. Show sample job data
    print("\n📋 SAMPLE JOB DATA:")
    if data['jobs']:
        sample_job = data['jobs'][0]
        print(f"Title: {sample_job.get('title', 'N/A')}")
        print(f"Company: {sample_job.get('company_name', 'N/A')}")
        print(f"Location: {sample_job.get('location_display', 'N/A')}")
        print(f"Salary Min: ${sample_job.get('salary_min', 'N/A')}")
        print(f"Salary Max: ${sample_job.get('salary_max', 'N/A')}")
        print(f"Category: {sample_job.get('category_label', 'N/A')}")
    
    # 3. Show salary histogram sample
    print("\n💰 SALARY HISTOGRAM SAMPLE:")
    if data['salary_histograms']:
        hist = data['salary_histograms'][0]
        print(f"Job Type: {hist['job_type']}")
        print(f"Histogram entries: {len(hist['histogram_data']['histogram']) if hist['histogram_data'] and 'histogram' in hist['histogram_data'] else 'N/A'}")
    
    # 4. Show top companies sample
    print("\n🏢 TOP COMPANIES SAMPLE:")
    if data['top_companies']:
        companies = data['top_companies'][0]
        print(f"Job Type: {companies['job_type']}")
        if companies['companies']:
            top_company = companies['companies'][0]
            print(f"Top Company: {top_company.get('display_name', 'N/A')}")
            print(f"Job Count: {top_company.get('count', 'N/A')}")
            print(f"Avg Salary: ${top_company.get('average_salary', 'N/A')}")
    
    # 5. Show available countries and categories
    print("\n🌍 AVAILABLE CATEGORIES:")
    if data['categories']:
        for i, cat in enumerate(data['categories'][:10]):  # Show first 10
            print(f"  {cat.get('label', 'N/A')} ({cat.get('tag', 'N/A')})")
        if len(data['categories']) > 10:
            print(f"  ... and {len(data['categories']) - 10} more categories")
    
    # 6. Create quick analysis CSV files
    print("\n📊 CREATING ANALYSIS FILES:")
    
    # Jobs DataFrame
    jobs_df = pd.DataFrame(data['jobs'])
    if not jobs_df.empty:
        # Select key columns for analysis
        analysis_columns = [
            'title', 'company_name', 'location_display', 'salary_min', 'salary_max',
            'category_label', 'contract_type', 'created', 'search_keyword', 'search_location'
        ]
        available_columns = [col for col in analysis_columns if col in jobs_df.columns]
        jobs_analysis = jobs_df[available_columns]
        jobs_analysis.to_csv(f'data/jobs_analysis_{time.strftime("%Y%m%d_%H%M%S")}.csv', index=False)
        print(f"✅ Jobs analysis saved to: data/jobs_analysis_{time.strftime('%Y%m%d_%H%M%S')}.csv")
        
        # Basic statistics
        print(f"\n📈 QUICK INSIGHTS:")
        print(f"Unique companies: {jobs_df['company_name'].nunique() if 'company_name' in jobs_df else 'N/A'}")
        print(f"Unique locations: {jobs_df['location_display'].nunique() if 'location_display' in jobs_df else 'N/A'}")
        print(f"Jobs with salary info: {jobs_df['salary_max'].notna().sum() if 'salary_max' in jobs_df else 'N/A'}")
        if 'salary_max' in jobs_df and jobs_df['salary_max'].notna().any():
            print(f"Avg max salary: ${jobs_df['salary_max'].mean():.0f}")
            print(f"Salary range: ${jobs_df['salary_max'].min():.0f} - ${jobs_df['salary_max'].max():.0f}")
    
    print(f"\n🎯 DATA AVAILABILITY INSIGHTS:")
    print(f"Countries supported: US, GB, AU, CA, DE, FR, etc. (19 total)")
    print(f"Your data covers: {len(set([job.get('search_location') for job in data['jobs']]))} locations")
    print(f"Job types extracted: {len(set([job.get('search_keyword') for job in data['jobs']]))} types")
    print(f"Categories available: {len(data['categories'])} categories")
    
    print(f"\n💡 NEXT STEPS:")
    print(f"1. Check data/jobs_analysis.csv for quick analysis")
    print(f"2. Use comprehensive_job_data.json for full data")
    print(f"3. Run: python -m src.loaders.sqlite_loader to load into database")
    print(f"4. Create Streamlit dashboard for visualization")