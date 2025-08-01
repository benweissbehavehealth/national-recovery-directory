# 🏥 National Recovery Support Services Directory

A comprehensive database and analytics platform for recovery support services across the United States, featuring 41,097 organizations with complete data lineage tracking.

## 🌟 Overview

This project provides a centralized directory of recovery support services including:
- **Treatment Centers** (inpatient, outpatient, residential)
- **Recovery Community Organizations** (RCOs)
- **Recovery Community Centers** (RCCs)
- **Oxford Houses** (sober living)
- **NARR Certified Residences**

## 📊 Data Coverage

- **41,097 organizations** across 50+ states and territories
- **7 data sources** with complete lineage tracking
- **9 certification authorities** (5 NARR affiliates)
- **13 organization type classifications**
- **Temporal tracking** for updates and confirmations

## 🚀 Quick Start

### 1. View Data with DuckDB UI
```bash
cd 04_processed_data/duckdb
./launch_ui.sh
```
Access at: http://localhost:4213/

### 2. Cloud Database (MotherDuck)
```bash
cd 04_processed_data/duckdb
./push_to_motherduck.sh
```

### 3. Python Analysis
```python
import duckdb
conn = duckdb.connect('04_processed_data/duckdb/databases/recovery_directory.duckdb')

# Sample query
df = conn.execute("""
    SELECT organization_type, COUNT(*) as count 
    FROM organizations 
    GROUP BY organization_type 
    ORDER BY count DESC
""").fetchdf()
```

## 📁 Project Structure

```
narr_extractor/
├── 01_documentation/          # Project documentation and plans
├── 02_scripts/               # Data extraction and processing scripts
│   ├── extraction/           # Web scrapers and data extractors
│   ├── aggregation/          # Data aggregation and integration
│   ├── analysis/             # Analytics and clustering
│   └── utilities/            # Helper utilities
├── 03_raw_data/             # Raw extracted data
│   ├── narr_organizations/   # NARR affiliate data
│   ├── oxford_house_data/    # Oxford House network
│   ├── recovery_centers/     # RCC directories
│   ├── recovery_organizations/ # RCO networks
│   └── treatment_centers/    # Treatment facility data
├── 04_processed_data/        # Processed and integrated data
│   ├── duckdb/              # DuckDB database and UI
│   └── master_directories/   # Aggregated master files
└── 05_reports/              # Analysis reports and visualizations
```

## 🔍 Data Sources

### Primary Sources
- **SAMHSA** - Substance Abuse and Mental Health Services Administration
- **NARR** - National Alliance for Recovery Residences
- **Oxford House** - Sober living network
- **State Directories** - Regional treatment directories
- **Recovery Networks** - Community organizations

### Certification Authorities
- **NARR Affiliates** (5 major affiliates)
- **State Certification Bodies**
- **Specialized Certifications**

## 🛠️ Technology Stack

- **Database**: DuckDB (local) + MotherDuck (cloud)
- **Extraction**: Python, Selenium, BeautifulSoup
- **Processing**: Pandas, NumPy
- **Analytics**: Network analysis, clustering
- **UI**: DuckDB Web UI, Jupyter notebooks

## 📈 Key Features

### Data Lineage Tracking
- Complete source attribution
- Update history and timestamps
- Quality metrics and validation
- Certification status tracking

### Analytics Capabilities
- Geographic distribution analysis
- Service type clustering
- Network analysis of recovery communities
- Quality assessment dashboards

### Cloud Integration
- MotherDuck cloud database
- Automated sync capabilities
- Cost-optimized storage (~$25/month)
- Team collaboration features

## 🔧 Installation

### Prerequisites
```bash
# Install Python dependencies
pip install -r 02_scripts/utilities/requirements.txt

# Install DuckDB
brew install duckdb  # macOS
```

### Setup
```bash
# Clone repository
git clone https://github.com/yourusername/national-recovery-directory.git
cd national-recovery-directory

# Initialize database
cd 04_processed_data/duckdb
python3 scripts/migration/json_to_duckdb.py
```

## 📊 Sample Queries

### Organization Overview
```sql
-- Count by type
SELECT organization_type, COUNT(*) as count 
FROM organizations 
GROUP BY organization_type 
ORDER BY count DESC;

-- Top states
SELECT * FROM state_summary 
ORDER BY organization_count DESC 
LIMIT 10;
```

### Data Quality
```sql
-- Quality dashboard
SELECT * FROM data_quality_dashboard;

-- Certification summary
SELECT * FROM certification_summary;
```

### Geographic Analysis
```sql
-- Find California treatment centers
SELECT * FROM organizations 
WHERE organization_type = 'treatment_centers' 
AND address_state = 'CA'
LIMIT 100;
```

## 🌐 Cloud Deployment

### MotherDuck Setup
1. Create account at https://motherduck.com
2. Get authentication token
3. Run migration: `./push_to_motherduck.sh`
4. Connect: `duckdb md:narr_directory?motherduck_token=YOUR_TOKEN`

### Cost Optimization
- **Storage**: ~$0.025/month (0.5 GB)
- **Compute**: ~$25.00/month (100 hours)
- **Total**: ~$25.03/month

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-data-source`
3. Add your changes
4. Test with sample queries
5. Submit pull request

### Data Source Guidelines
- Document extraction methodology
- Include data quality metrics
- Maintain lineage tracking
- Follow privacy guidelines

## 📋 Roadmap

- [ ] **Real-time Updates** - Automated data refresh
- [ ] **API Development** - RESTful API for data access
- [ ] **Mobile App** - Directory mobile application
- [ ] **Advanced Analytics** - Machine learning insights
- [ ] **International Expansion** - Global recovery networks

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **NARR** for certification standards
- **SAMHSA** for treatment facility data
- **Oxford House** for sober living network
- **Recovery Community Organizations** for grassroots data

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/national-recovery-directory/issues)
- **Documentation**: [Wiki](https://github.com/yourusername/national-recovery-directory/wiki)
- **Community**: [Discussions](https://github.com/yourusername/national-recovery-directory/discussions)

---

**Mission**: To provide comprehensive, accurate, and accessible information about recovery support services to help individuals and families find the resources they need for successful recovery journeys.

**Data Last Updated**: July 31, 2025  
**Total Organizations**: 41,097  
**Coverage**: 50+ states and territories 