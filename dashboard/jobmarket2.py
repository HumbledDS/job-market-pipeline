import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from config.settings import Settings

# Add project root to path
sys.path.append('..')
from config.settings import Settings

class JobMarketDashboard:
    """
    Streamlit dashboard for job market analytics
    Works with actual available data in your SQLite database
    """
    
    def __init__(self):
        self.settings = Settings()
        st.set_page_config(
            page_title="Job Market Analytics",
            page_icon="💼",
            layout="wide"
        )
    
    def get_available_tables_and_views(self):
        """Check what data is actually available"""
        conn = sqlite3.connect(self.settings.DATABASE_PATH)
        
        # Get all tables and views
        tables_df = pd.read_sql_query("""
            SELECT name, type FROM sqlite_master 
            WHERE type IN ('table', 'view')
            ORDER BY type, name
        """, conn)
        
        conn.close()
        return tables_df
    
    def load_raw_jobs_data(self):
        """Load data directly from raw_jobs table"""
        conn = sqlite3.connect(self.settings.DATABASE_PATH)
        
        # Get column info
        columns_df = pd.read_sql_query("PRAGMA table_info(raw_jobs)", conn)
        available_columns = columns_df['name'].tolist()
        
        # Build query based on available columns
        select_columns = ['id', 'title', 'company', 'location']
        
        if 'salary_min' in available_columns:
            select_columns.append('salary_min')
        if 'salary_max' in available_columns:
            select_columns.append('salary_max')
        if 'category' in available_columns:
            select_columns.append('category')
        if 'created' in available_columns:
            select_columns.append('created')
        if 'search_keyword' in available_columns:
            select_columns.append('search_keyword')
        if 'search_location' in available_columns:
            select_columns.append('search_location')
        if 'seniority_level' in available_columns:
            select_columns.append('seniority_level')
        if 'is_remote' in available_columns:
            select_columns.append('is_remote')
        if 'skills_extracted' in available_columns:
            select_columns.append('skills_extracted')
        
        query = f"""
            SELECT {', '.join(select_columns)}
            FROM raw_jobs 
            WHERE salary_max > 1000 
            ORDER BY created DESC
            LIMIT 1000
        """
        
        jobs_df = pd.read_sql_query(query, conn)
        conn.close()
        
        return jobs_df, available_columns
    
    def run_dashboard(self):
        """Main dashboard application"""
        st.title("💼 Job Market Analytics Dashboard")
        st.markdown("Real-time insights from your job market data")
        
        # Show available data structure
        st.sidebar.header("📊 Database Info")
        tables_df = self.get_available_tables_and_views()
        st.sidebar.write("**Available Data:**")
        for _, row in tables_df.iterrows():
            st.sidebar.write(f"• {row['name']} ({row['type']})")
        
        # Load actual data
        try:
            jobs_df, available_columns = self.load_raw_jobs_data()
            st.sidebar.write(f"**Jobs loaded:** {len(jobs_df):,}")
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return
        
        if jobs_df.empty:
            st.warning("No job data available!")
            return
        
        # Data summary
        st.subheader("📈 Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Jobs", f"{len(jobs_df):,}")
        
        with col2:
            if 'salary_max' in jobs_df.columns:
                avg_salary = jobs_df['salary_max'].mean()
                st.metric("Avg Max Salary", f"${avg_salary:,.0f}")
            else:
                st.metric("Avg Salary", "No salary data")
        
        with col3:
            unique_companies = jobs_df['company'].nunique()
            st.metric("Unique Companies", f"{unique_companies:,}")
        
        with col4:
            unique_locations = jobs_df['location'].nunique()
            st.metric("Unique Locations", f"{unique_locations:,}")
        
        # Filters
        st.sidebar.header("🔍 Filters")
        
        # Company filter
        if len(jobs_df) > 0:
            companies = ['All'] + sorted(jobs_df['company'].dropna().unique().tolist())
            selected_company = st.sidebar.selectbox("Company", companies)
            
            # Location filter
            locations = ['All'] + sorted(jobs_df['location'].dropna().unique().tolist())
            selected_location = st.sidebar.selectbox("Location", locations)
            
            # Apply filters
            filtered_df = jobs_df.copy()
            if selected_company != 'All':
                filtered_df = filtered_df[filtered_df['company'] == selected_company]
            if selected_location != 'All':
                filtered_df = filtered_df[filtered_df['location'] == selected_location]
        else:
            filtered_df = jobs_df
        
        # Charts
        if len(filtered_df) > 0:
            # Chart 1: Top Companies
            st.subheader("🏢 Top Hiring Companies")
            company_counts = filtered_df['company'].value_counts().head(15)
            if len(company_counts) > 0:
                fig_companies = px.bar(
                    x=company_counts.values,
                    y=company_counts.index,
                    orientation='h',
                    title="Companies with Most Job Postings",
                    labels={'x': 'Number of Jobs', 'y': 'Company'}
                )
                fig_companies.update_layout(height=500)
                st.plotly_chart(fig_companies, use_container_width=True)
            
            # Chart 2: Salary Distribution (if available)
            if 'salary_max' in filtered_df.columns:
                st.subheader("💰 Salary Distribution")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Salary histogram
                    fig_salary_hist = px.histogram(
                        filtered_df,
                        x='salary_max',
                        title="Salary Distribution",
                        nbins=30
                    )
                    st.plotly_chart(fig_salary_hist, use_container_width=True)
                
                with col2:
                    # Salary by company (top 10)
                    top_companies = company_counts.head(10).index
                    salary_by_company = filtered_df[filtered_df['company'].isin(top_companies)].groupby('company')['salary_max'].mean().sort_values(ascending=False)
                    
                    fig_salary_company = px.bar(
                        x=salary_by_company.values,
                        y=salary_by_company.index,
                        orientation='h',
                        title="Average Max Salary by Company (Top 10)",
                        labels={'x': 'Average Max Salary', 'y': 'Company'}
                    )
                    st.plotly_chart(fig_salary_company, use_container_width=True)
            
            # Chart 3: Geographic Distribution
            st.subheader("📍 Geographic Distribution")
            location_counts = filtered_df['location'].value_counts().head(15)
            if len(location_counts) > 0:
                fig_locations = px.pie(
                    values=location_counts.values,
                    names=location_counts.index,
                    title="Jobs by Location"
                )
                st.plotly_chart(fig_locations, use_container_width=True)
            
            # Chart 4: Search Keywords (if available)
            if 'search_keyword' in filtered_df.columns:
                st.subheader("🔍 Job Types Searched")
                keyword_counts = filtered_df['search_keyword'].value_counts()
                if len(keyword_counts) > 0:
                    fig_keywords = px.bar(
                        x=keyword_counts.index,
                        y=keyword_counts.values,
                        title="Jobs by Search Keyword"
                    )
                    st.plotly_chart(fig_keywords, use_container_width=True)
        
        # Data Table
        st.subheader("📋 Job Listings")
        
        # Column selector
        display_columns = st.multiselect(
            "Select columns to display:",
            options=[col for col in filtered_df.columns if col in ['title', 'company', 'location', 'salary_max', 'salary_min', 'category', 'search_keyword']],
            default=[col for col in ['title', 'company', 'location', 'salary_max'] if col in filtered_df.columns]
        )
        
        if display_columns:
            # Show data with pagination
            page_size = 50
            total_rows = len(filtered_df)
            total_pages = (total_rows - 1) // page_size + 1
            
            if total_pages > 1:
                page = st.selectbox("Page", range(1, total_pages + 1))
                start_idx = (page - 1) * page_size
                end_idx = start_idx + page_size
                display_data = filtered_df[display_columns].iloc[start_idx:end_idx]
            else:
                display_data = filtered_df[display_columns].head(page_size)
            
            st.dataframe(display_data, use_container_width=True)
            
            # Download option
            st.subheader("📥 Export Data")
            if st.button("Download Filtered Data as CSV"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"job_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        # Database Schema Info
        st.subheader("🗄️ Database Schema")
        if st.checkbox("Show available columns"):
            st.write("**Available columns in raw_jobs table:**")
            for col in available_columns:
                st.write(f"• {col}")

# Run the dashboard
if __name__ == "__main__":
    dashboard = JobMarketDashboard()
    dashboard.run_dashboard()