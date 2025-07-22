import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from config.settings import Settings



# Add project root to path
sys.path.append('..')
from config.settings import Settings

class JobMarketDashboard:
    """
    Streamlit dashboard for job market analytics
    
    Educational Notes:
    - Streamlit makes creating web apps simple
    - Plotly provides interactive charts
    - Using SQLite instead of DuckDB for Windows compatibility
    """
    
    def __init__(self):
        self.settings = Settings()
        st.set_page_config(
            page_title="Job Market Analytics",
            page_icon="💼",
            layout="wide"
        )
    
    def load_data(self):
        """Load data from SQLite (removed caching to fix error)"""
        conn = sqlite3.connect(self.settings.DATABASE_PATH)
        
        # Load staging jobs data
        jobs_df = pd.read_sql_query("""
            SELECT * FROM stg_jobs 
            ORDER BY job_posted_date DESC
        """, conn)
        
        # Load company data
        companies_df = pd.read_sql_query("""
            SELECT * FROM dim_companies 
            ORDER BY total_jobs_posted DESC
            LIMIT 20
        """, conn)
        
        # Load skills analysis
        skills_df = pd.read_sql_query("""
            SELECT * FROM skills_analysis 
            ORDER BY job_count DESC
            LIMIT 20
        """, conn)
        
        conn.close()
        return jobs_df, companies_df, skills_df
    
    def run_dashboard(self):
        """Main dashboard application"""
        st.title("💼 Job Market Analytics Dashboard")
        st.markdown("Real-time insights from job market data")
        
        # Load data
        try:
            jobs_df, companies_df, skills_df = self.load_data()
        except Exception as e:
            st.error(f"Error loading data: {e}")
            st.info("Make sure you've run the pipeline and created analytics views first!")
            return
        
        if jobs_df.empty:
            st.warning("No data available. Please run the pipeline first!")
            return
        
        # Sidebar filters
        st.sidebar.header("Filters")
        
        # Location filter
        locations = jobs_df['location'].dropna().unique()
        selected_location = st.sidebar.selectbox("Select Location", ["All"] + list(locations))
        
        # Seniority filter
        seniority_levels = jobs_df['seniority_level'].dropna().unique()
        selected_seniority = st.sidebar.selectbox("Seniority Level", ["All"] + list(seniority_levels))
        
        # Apply filters
        filtered_df = jobs_df.copy()
        if selected_location != "All":
            filtered_df = filtered_df[filtered_df['location'] == selected_location]
        if selected_seniority != "All":
            filtered_df = filtered_df[filtered_df['seniority_level'] == selected_seniority]
        
        # Key Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_jobs = len(filtered_df)
            st.metric("Total Jobs", f"{total_jobs:,}")
        
        with col2:
            avg_salary = filtered_df['salary_max'].mean() if len(filtered_df) > 0 else 0
            st.metric("Avg Max Salary", f"${avg_salary:,.0f}")
        
        with col3:
            if len(filtered_df) > 0 and 'is_remote' in filtered_df.columns:
                remote_count = filtered_df['is_remote'].sum()
                remote_pct = (remote_count / len(filtered_df) * 100)
                st.metric("Remote Jobs %", f"{remote_pct:.1f}%")
            else:
                st.metric("Remote Jobs %", "N/A")
        
        with col4:
            unique_companies = filtered_df['company'].nunique()
            st.metric("Companies Hiring", f"{unique_companies:,}")
        
        # Charts Row 1
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💰 Salary Distribution by Seniority")
            if 'seniority_level' in filtered_df.columns and len(filtered_df) > 0:
                salary_box = px.box(
                    filtered_df, 
                    x='seniority_level', 
                    y='salary_max',
                    title="Salary Ranges by Experience Level"
                )
                salary_box.update_layout(height=400)
                st.plotly_chart(salary_box, use_container_width=True)
            else:
                st.info("No seniority data available")
        
        with col2:
            st.subheader("🏢 Top Hiring Companies")
            if len(companies_df) > 0:
                company_bar = px.bar(
                    companies_df.head(10),
                    x='total_jobs_posted',
                    y='company',
                    orientation='h',
                    title="Companies with Most Job Postings"
                )
                company_bar.update_layout(height=400)
                st.plotly_chart(company_bar, use_container_width=True)
            else:
                st.info("No company data available")
        
        # Charts Row 2
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📍 Jobs by Location")
            if len(filtered_df) > 0:
                location_counts = filtered_df['location'].value_counts().head(10)
                if len(location_counts) > 0:
                    location_pie = px.pie(
                        values=location_counts.values,
                        names=location_counts.index,
                        title="Geographic Distribution of Jobs"
                    )
                    st.plotly_chart(location_pie, use_container_width=True)
                else:
                    st.info("No location data available")
        
        with col2:
            st.subheader("🛠️ Top Skills Demand")
            if len(skills_df) > 0:
                skills_bar = px.bar(
                    skills_df.head(10),
                    x='job_count',
                    y='skill_name',
                    orientation='h',
                    title="Most In-Demand Skills"
                )
                skills_bar.update_layout(height=400)
                st.plotly_chart(skills_bar, use_container_width=True)
            else:
                st.info("No skills data available")
        
        # Detailed Data Table
        st.subheader("📋 Recent Job Postings")
        
        # Display options
        available_cols = ['title', 'company', 'location', 'salary_max', 'seniority_level']
        if 'is_remote' in filtered_df.columns:
            available_cols.append('is_remote')
            
        display_cols = st.multiselect(
            "Select columns to display:",
            options=available_cols,
            default=['title', 'company', 'location', 'salary_max']
        )
        
        if display_cols and len(filtered_df) > 0:
            existing_cols = [col for col in display_cols if col in filtered_df.columns]
            if existing_cols:
                st.dataframe(
                    filtered_df[existing_cols].head(50),
                    use_container_width=True
                )

# Run the dashboard
if __name__ == "__main__":
    dashboard = JobMarketDashboard()
    dashboard.run_dashboard()