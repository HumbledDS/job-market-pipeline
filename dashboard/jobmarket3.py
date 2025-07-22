import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import glob
import os
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from config.settings import Settings


class JobMarketDashboard:
    """
    Streamlit dashboard for job market analytics
    Uses CSV/JSON files instead of database for easy deployment
    """
    
    def __init__(self):
        st.set_page_config(
            page_title="Job Market Analytics",
            page_icon="💼",
            layout="wide",
            initial_sidebar_state="collapsed"
        )
        
        # Custom CSS for better appearance
        st.markdown("""
        <style>
            .main > div {
                padding-top: 1rem;
            }
            .metric-card {
                background-color: #f0f2f6;
                padding: 1rem;
                border-radius: 0.5rem;
                text-align: center;
            }
            .stPlotlyChart {
                background-color: white;
                border-radius: 0.5rem;
                padding: 0.5rem;
            }
        </style>
        """, unsafe_allow_html=True)
    
    def find_latest_data_file(self):
        """Find the most recent data file (CSV or JSON)"""
        # Look for specific CSV files first
        csv_files = glob.glob("data/jobs_analysis_*.csv")
        json_files = glob.glob("data/comprehensive_job_data_*.json")
        
        if csv_files:
            latest_csv = sorted(csv_files)[-1]
            return latest_csv, 'csv'
        elif json_files:
            latest_json = sorted(json_files)[-1]
            return latest_json, 'json'
        else:
            # Fallback to any CSV or JSON in data folder
            all_csvs = glob.glob("data/*.csv")
            all_jsons = glob.glob("data/*.json")
            
            if all_csvs:
                return sorted(all_csvs)[-1], 'csv'
            elif all_jsons:
                return sorted(all_jsons)[-1], 'json'
            else:
                return None, None
    
    def load_data_from_csv(self, file_path):
        """Load data from CSV file with proper column mapping"""
        try:
            df = pd.read_csv(file_path)
            
            # Map your actual column names to standard names
            column_mapping = {
                'company_name': 'company',
                'location_display': 'location',
                'category_label': 'category'
            }
            
            # Apply column mapping
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df[new_col] = df[old_col]
            
            # Clean and standardize numeric columns
            if 'salary_max' in df.columns:
                df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
            if 'salary_min' in df.columns:
                df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
            
            # Clean text columns
            text_columns = ['title', 'company', 'location', 'category']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].fillna('Unknown').astype(str)
            
            # Filter out unrealistic salaries
            if 'salary_max' in df.columns:
                df = df[(df['salary_max'] > 1000) & (df['salary_max'] < 1000000)]
            
            return df
            
        except Exception as e:
            st.error(f"Error loading CSV: {e}")
            return pd.DataFrame()
    
    def load_data_from_json(self, file_path):
        """Load data from JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract jobs data
            if isinstance(data, dict) and 'jobs' in data:
                jobs_data = data['jobs']
            elif isinstance(data, list):
                jobs_data = data
            else:
                st.error("Unexpected JSON structure")
                return pd.DataFrame()
            
            df = pd.DataFrame(jobs_data)
            
            # Map column names
            column_mapping = {
                'company_name': 'company',
                'location_display': 'location',
                'category_label': 'category'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df[new_col] = df[old_col]
            
            # Clean numeric columns
            if 'salary_max' in df.columns:
                df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
            if 'salary_min' in df.columns:
                df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
            
            # Clean text columns
            text_columns = ['title', 'company', 'location', 'category']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].fillna('Unknown').astype(str)
            
            # Filter out unrealistic salaries
            if 'salary_max' in df.columns:
                df = df[(df['salary_max'] > 1000) & (df['salary_max'] < 1000000)]
            
            return df
            
        except Exception as e:
            st.error(f"Error loading JSON: {e}")
            return pd.DataFrame()
    
    def extract_skills_from_text(self, df):
        """Extract skills from job titles and descriptions"""
        skills_dict = {
            'Python': ['python', 'py'],
            'SQL': ['sql', 'mysql', 'postgresql', 'postgres'],
            'AWS': ['aws', 'amazon web services'],
            'JavaScript': ['javascript', 'js', 'node.js', 'nodejs'],
            'React': ['react', 'reactjs'],
            'Java': [' java ', 'java,', 'java.'],
            'Docker': ['docker', 'container'],
            'Kubernetes': ['kubernetes', 'k8s'],
            'Machine Learning': ['machine learning', 'ml', 'ai'],
            'Tableau': ['tableau'],
            'Power BI': ['power bi', 'powerbi'],
            'Excel': ['excel'],
            'Git': ['git', 'github'],
            'Linux': ['linux', 'unix'],
            'Spark': ['spark', 'apache spark']
        }
        
        # Combine text from title and description columns
        text_columns = []
        if 'title' in df.columns:
            text_columns.append('title')
        if 'description' in df.columns:
            text_columns.append('description')
        
        if not text_columns:
            return {}
        
        # Combine all text
        all_text = ''
        for col in text_columns:
            all_text += ' ' + ' '.join(df[col].fillna('').astype(str))
        
        all_text = all_text.lower()
        
        skill_counts = {}
        for skill, keywords in skills_dict.items():
            count = sum(all_text.count(keyword) for keyword in keywords)
            if count > 0:
                skill_counts[skill] = count
        
        return skill_counts
    
    def run_dashboard(self):
        """Main dashboard application"""
        st.title("💼 Job Market Analytics Dashboard")
        st.markdown("*Real-time insights from job market data*")
        
        # Load data
        file_path, file_type = self.find_latest_data_file()
        
        if not file_path:
            st.error("No data files found! Please ensure you have CSV or JSON files in the 'data' folder.")
            st.info("Expected files: `data/jobs_analysis_*.csv` or `data/comprehensive_job_data_*.json`")
            return
        
        # Show data source
        st.info(f"📁 Using data from: `{os.path.basename(file_path)}` ({file_type.upper()})")
        
        # Load data based on file type
        if file_type == 'csv':
            df = self.load_data_from_csv(file_path)
        else:
            df = self.load_data_from_json(file_path)
        
        if df.empty:
            st.error("No data could be loaded from the file.")
            return
        
        # Data overview
        st.success(f"✅ Loaded {len(df):,} job records")
        
        # Debug info (optional - can remove for production)
        with st.expander("🔍 Debug: View Data Structure"):
            st.write("**Available columns:**", list(df.columns))
            st.write("**Sample data:**")
            st.dataframe(df.head(3))
        
        # Sidebar filters
        st.sidebar.header("🔍 Filters")
        
        # Company filter
        if 'company' in df.columns:
            companies = ['All'] + sorted([comp for comp in df['company'].dropna().unique() if comp != 'Unknown'])
            selected_company = st.sidebar.selectbox("Company", companies)
        else:
            selected_company = 'All'
        
        # Location filter
        if 'location' in df.columns:
            locations = ['All'] + sorted([loc for loc in df['location'].dropna().unique() if loc != 'Unknown'])
            selected_location = st.sidebar.selectbox("Location", locations)
        else:
            selected_location = 'All'
        
        # Search keyword filter
        if 'search_keyword' in df.columns:
            keywords = ['All'] + sorted(df['search_keyword'].dropna().unique().tolist())
            selected_keyword = st.sidebar.selectbox("Job Type", keywords)
        else:
            selected_keyword = 'All'
        
        # Salary range filter
        if 'salary_max' in df.columns and not df['salary_max'].isna().all():
            min_salary = int(df['salary_max'].min())
            max_salary = int(df['salary_max'].max())
            salary_range = st.sidebar.slider(
                "Salary Range ($)",
                min_value=min_salary,
                max_value=max_salary,
                value=(min_salary, max_salary)
            )
        else:
            salary_range = None
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_company != 'All':
            filtered_df = filtered_df[filtered_df['company'] == selected_company]
        
        if selected_location != 'All':
            filtered_df = filtered_df[filtered_df['location'] == selected_location]
        
        if selected_keyword != 'All' and 'search_keyword' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['search_keyword'] == selected_keyword]
        
        if salary_range and 'salary_max' in filtered_df.columns:
            filtered_df = filtered_df[
                (filtered_df['salary_max'] >= salary_range[0]) & 
                (filtered_df['salary_max'] <= salary_range[1])
            ]
        
        # Key Metrics
        st.subheader("📊 Key Metrics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Jobs", f"{len(filtered_df):,}")
        
        with col2:
            if 'salary_max' in filtered_df.columns and not filtered_df['salary_max'].isna().all():
                avg_salary = filtered_df['salary_max'].mean()
                st.metric("Avg Max Salary", f"${avg_salary:,.0f}")
            else:
                st.metric("Avg Salary", "No data")
        
        with col3:
            if 'company' in filtered_df.columns:
                unique_companies = filtered_df[filtered_df['company'] != 'Unknown']['company'].nunique()
                st.metric("Companies", f"{unique_companies:,}")
            else:
                st.metric("Companies", "No data")
        
        with col4:
            if 'location' in filtered_df.columns:
                unique_locations = filtered_df[filtered_df['location'] != 'Unknown']['location'].nunique()
                st.metric("Locations", f"{unique_locations:,}")
            else:
                st.metric("Locations", "No data")
        
        # Charts Row 1
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏢 Top Hiring Companies")
            if 'company' in filtered_df.columns and len(filtered_df) > 0:
                company_counts = filtered_df[filtered_df['company'] != 'Unknown']['company'].value_counts().head(10)
                if len(company_counts) > 0:
                    fig = px.bar(
                        x=company_counts.values,
                        y=company_counts.index,
                        orientation='h',
                        title="Companies with Most Job Postings",
                        labels={'x': 'Number of Jobs', 'y': 'Company'}
                    )
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No company data available")
            else:
                st.info("No company data available")
        
        with col2:
            st.subheader("💰 Salary Distribution")
            if 'salary_max' in filtered_df.columns and not filtered_df['salary_max'].isna().all():
                salary_data = filtered_df['salary_max'].dropna()
                if len(salary_data) > 0:
                    fig = px.histogram(
                        x=salary_data,
                        nbins=20,
                        title="Salary Distribution",
                        labels={'x': 'Salary ($)', 'y': 'Number of Jobs'}
                    )
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No salary data available")
            else:
                st.info("No salary data available")
        
        # Charts Row 2
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📍 Geographic Distribution")
            if 'location' in filtered_df.columns and len(filtered_df) > 0:
                location_counts = filtered_df[filtered_df['location'] != 'Unknown']['location'].value_counts().head(10)
                if len(location_counts) > 0:
                    fig = px.pie(
                        values=location_counts.values,
                        names=location_counts.index,
                        title="Jobs by Location"
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No location data available")
            else:
                st.info("No location data available")
        
        with col2:
            st.subheader("🛠️ Skills Demand")
            skills = self.extract_skills_from_text(filtered_df)
            
            if skills:
                skills_df = pd.DataFrame(list(skills.items()), columns=['Skill', 'Mentions'])
                skills_df = skills_df.sort_values('Mentions', ascending=False).head(10)
                
                fig = px.bar(
                    skills_df,
                    x='Mentions',
                    y='Skill',
                    orientation='h',
                    title="Most Mentioned Skills",
                    labels={'Mentions': 'Number of Mentions', 'Skill': 'Skill'}
                )
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No skills data could be extracted")
        
        # Charts Row 3
        if 'search_keyword' in filtered_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🎯 Job Categories")
                keyword_counts = filtered_df['search_keyword'].value_counts()
                if len(keyword_counts) > 0:
                    fig = px.bar(
                        x=keyword_counts.index,
                        y=keyword_counts.values,
                        title="Jobs by Search Keyword",
                        labels={'x': 'Job Type', 'y': 'Number of Jobs'}
                    )
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'salary_max' in filtered_df.columns:
                    st.subheader("💵 Salary by Job Type")
                    salary_by_keyword = filtered_df.groupby('search_keyword')['salary_max'].mean().sort_values(ascending=False)
                    if len(salary_by_keyword) > 0:
                        fig = px.bar(
                            x=salary_by_keyword.index,
                            y=salary_by_keyword.values,
                            title="Average Salary by Job Type",
                            labels={'x': 'Job Type', 'y': 'Average Salary ($)'}
                        )
                        fig.update_layout(height=400, showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
        
        # Data Table
        st.subheader("📋 Job Listings")
        
        # Column selector
        available_columns = [col for col in ['title', 'company', 'location', 'salary_max', 'salary_min', 'search_keyword', 'category'] if col in filtered_df.columns]
        
        if available_columns:
            selected_columns = st.multiselect(
                "Select columns to display:",
                options=available_columns,
                default=available_columns[:4]
            )
            
            if selected_columns:
                # Show total count
                st.caption(f"Showing filtered results: {len(filtered_df):,} jobs")
                
                # Pagination
                page_size = 25
                total_rows = len(filtered_df)
                
                if total_rows > page_size:
                    total_pages = (total_rows - 1) // page_size + 1
                    page = st.selectbox("Page", range(1, total_pages + 1))
                    start_idx = (page - 1) * page_size
                    end_idx = min(start_idx + page_size, total_rows)
                    display_data = filtered_df[selected_columns].iloc[start_idx:end_idx]
                    st.caption(f"Showing {start_idx + 1}-{end_idx} of {total_rows} jobs")
                else:
                    display_data = filtered_df[selected_columns]
                
                st.dataframe(display_data, use_container_width=True)
        
        # Export functionality
        st.subheader("📥 Export Data")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📊 Download Filtered Data"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"filtered_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col2:
            st.info(f"📁 **Data Source**  \n{os.path.basename(file_path)}")
        
        with col3:
            st.info(f"📊 **Records Shown**  \n{len(filtered_df):,} of {len(df):,}")
        
        # Footer
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; color: #666;'>
            <p><em>Built with Python, Streamlit & Plotly</em></p>
            <p><a href='https://github.com/yourusername/job-market-pipeline' target='_blank'>🔗 View Source Code</a></p>
        </div>
        """, unsafe_allow_html=True)

# Run the dashboard
if __name__ == "__main__":
    dashboard = JobMarketDashboard()
    dashboard.run_dashboard()