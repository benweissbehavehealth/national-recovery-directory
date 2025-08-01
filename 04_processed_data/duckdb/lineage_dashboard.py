#!/usr/bin/env python3
"""
Simple web dashboard for viewing data lineage
"""

import duckdb
from pathlib import Path
from flask import Flask, render_template_string
import pandas as pd

app = Flask(__name__)

# Database connection
DB_PATH = Path(__file__).parent / "databases" / "recovery_directory.duckdb"

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Recovery Directory - Data Lineage Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        h1 { color: #333; }
        h2 { color: #666; margin-top: 30px; }
        table { border-collapse: collapse; width: 100%; background-color: white; margin-bottom: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .stats-box { background-color: white; padding: 20px; margin: 10px 0; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric { display: inline-block; margin: 10px 20px; }
        .metric-value { font-size: 2em; font-weight: bold; color: #4CAF50; }
        .metric-label { color: #666; }
    </style>
</head>
<body>
    <h1>🦆 Recovery Directory - Data Lineage Dashboard</h1>
    
    <div class="stats-box">
        <div class="metric">
            <div class="metric-value">{{ total_orgs }}</div>
            <div class="metric-label">Total Organizations</div>
        </div>
        <div class="metric">
            <div class="metric-value">{{ total_sources }}</div>
            <div class="metric-label">Data Sources</div>
        </div>
        <div class="metric">
            <div class="metric-value">{{ total_authorities }}</div>
            <div class="metric-label">Certification Authorities</div>
        </div>
    </div>
    
    <h2>📊 Data Sources</h2>
    {{ sources_table|safe }}
    
    <h2>🏢 Organization Types</h2>
    {{ org_types_table|safe }}
    
    <h2>📜 Certification Authorities</h2>
    {{ authorities_table|safe }}
    
    <h2>📈 Data Quality by Source</h2>
    {{ quality_table|safe }}
</body>
</html>
'''

def get_data():
    """Fetch data from database"""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    # Get totals
    total_orgs = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
    total_sources = conn.execute("SELECT COUNT(*) FROM data_sources").fetchone()[0]
    total_authorities = conn.execute("SELECT COUNT(*) FROM certification_authorities").fetchone()[0]
    
    # Get data sources
    sources_df = conn.execute("""
        SELECT 
            source_name as 'Source Name',
            source_type as 'Type',
            reliability_score as 'Reliability',
            update_frequency as 'Update Frequency',
            COALESCE(CAST(organizations_count AS VARCHAR), '0') as 'Organizations'
        FROM data_source_summary
        ORDER BY organizations_count DESC
    """).fetchdf()
    
    # Get organization types
    org_types_df = conn.execute("""
        SELECT 
            type_name as 'Type',
            COALESCE(parent_type_id, 'Top Level') as 'Parent',
            category as 'Category',
            CASE WHEN parent_type_id IS NULL THEN '✓' ELSE '' END as 'Main Type'
        FROM organization_types
        ORDER BY category, parent_type_id NULLS FIRST
    """).fetchdf()
    
    # Get authorities
    authorities_df = conn.execute("""
        SELECT 
            authority_name as 'Authority',
            authority_type as 'Type',
            jurisdiction_level as 'Jurisdiction',
            CAST(certification_types AS VARCHAR) as 'Cert Types',
            CASE WHEN is_narr_affiliate THEN 'Yes' ELSE 'No' END as 'NARR Affiliate'
        FROM certification_authority_summary
        ORDER BY certified_organizations DESC
    """).fetchdf()
    
    # Get data quality
    quality_df = conn.execute("""
        SELECT 
            source_id as 'Source',
            COUNT(*) as 'Total Records',
            COUNT(DISTINCT organization_id) as 'Unique Organizations',
            ROUND(100.0 * COUNT(DISTINCT organization_id) / COUNT(*), 2) || '%' as 'Uniqueness Rate'
        FROM organization_lineage
        WHERE is_current = TRUE
        GROUP BY source_id
        ORDER BY COUNT(*) DESC
    """).fetchdf()
    
    conn.close()
    
    return {
        'total_orgs': f"{total_orgs:,}",
        'total_sources': total_sources,
        'total_authorities': total_authorities,
        'sources_table': sources_df.to_html(index=False, escape=False),
        'org_types_table': org_types_df.to_html(index=False, escape=False),
        'authorities_table': authorities_df.to_html(index=False, escape=False),
        'quality_table': quality_df.to_html(index=False, escape=False)
    }

@app.route('/')
def dashboard():
    data = get_data()
    return render_template_string(HTML_TEMPLATE, **data)

if __name__ == '__main__':
    print("🌐 Starting Data Lineage Dashboard...")
    print("📍 Visit http://localhost:5000 to view the dashboard")
    app.run(debug=True, port=5000)