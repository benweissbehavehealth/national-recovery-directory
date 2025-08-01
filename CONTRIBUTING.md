# Contributing to National Recovery Support Services Directory

Thank you for your interest in contributing to the National Recovery Support Services Directory! This project aims to provide comprehensive, accurate, and accessible information about recovery support services to help individuals and families find the resources they need.

## 🚀 Quick Start

1. **Fork** the repository
2. **Clone** your fork: `git clone https://github.com/yourusername/national-recovery-directory.git`
3. **Create** a feature branch: `git checkout -b feature/amazing-feature`
4. **Make** your changes
5. **Test** your changes
6. **Commit** with a clear message: `git commit -m "Add amazing feature"`
7. **Push** to your fork: `git push origin feature/amazing-feature`
8. **Create** a Pull Request

## 📋 Development Setup

### Prerequisites
- Python 3.9+
- DuckDB
- Git

### Installation
```bash
# Clone the repository
git clone https://github.com/benweissbehavehealth/national-recovery-directory.git
cd national-recovery-directory

# Install Python dependencies
pip install -r 02_scripts/utilities/requirements.txt

# Install DuckDB (macOS)
brew install duckdb

# Initialize the database (if you have the data files)
cd 04_processed_data/duckdb
python3 scripts/migration/json_to_duckdb.py
```

## 🎯 Areas for Contribution

### High Priority
- **Data Source Extractions**: Add new recovery service directories
- **Data Quality Improvements**: Enhance validation and cleaning
- **API Development**: Create RESTful API for data access
- **Documentation**: Improve guides and tutorials

### Medium Priority
- **UI Enhancements**: Improve DuckDB web interface
- **Analytics**: Add new analysis and visualization features
- **Performance**: Optimize database queries and processing
- **Testing**: Add unit and integration tests

### Low Priority
- **Mobile App**: Develop mobile directory application
- **International Expansion**: Add non-US recovery networks
- **Advanced Analytics**: Machine learning insights
- **Community Features**: User reviews and ratings

## 📊 Data Source Contributions

### Adding New Data Sources

1. **Research the Source**
   - Identify the data source and its value
   - Assess data quality and completeness
   - Check technical requirements and limitations

2. **Create Extraction Script**
   - Place in `02_scripts/extraction/`
   - Follow naming convention: `{source}_{type}_extractor.py`
   - Include error handling and logging
   - Add data validation

3. **Update Documentation**
   - Document extraction methodology
   - Add to data sources list
   - Update lineage tracking

4. **Test Thoroughly**
   - Test with sample data
   - Verify data quality
   - Check for duplicates

### Data Source Guidelines

- **Respect Rate Limits**: Be mindful of website load
- **Follow Robots.txt**: Check website terms of service
- **Data Quality**: Validate and clean extracted data
- **Documentation**: Document any special requirements
- **Error Handling**: Graceful handling of failures

## 🧪 Testing

### Running Tests
```bash
# Install test dependencies
pip install pytest pytest-cov flake8

# Run linting
flake8 02_scripts --count --select=E9,F63,F7,F82 --show-source --statistics

# Run tests
pytest 02_scripts --cov=02_scripts --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Test Guidelines
- Write tests for new extraction scripts
- Test data validation functions
- Verify database operations
- Test error handling scenarios

## 📝 Code Style

### Python
- Follow PEP 8 style guide
- Use meaningful variable names
- Add docstrings to functions
- Keep functions focused and small
- Use type hints where helpful

### SQL
- Use consistent naming conventions
- Add comments for complex queries
- Optimize for performance
- Use parameterized queries

### Documentation
- Update README.md for new features
- Document API changes
- Add inline comments for complex logic
- Keep documentation current

## 🔄 Pull Request Process

### Before Submitting
1. **Test** your changes thoroughly
2. **Update** documentation as needed
3. **Check** that tests pass
4. **Verify** code style compliance
5. **Review** your own changes

### Pull Request Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Data source addition
- [ ] Performance improvement

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed
- [ ] Data quality verified

## Checklist
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] No breaking changes
```

## 🐛 Bug Reports

### Before Reporting
1. **Search** existing issues
2. **Reproduce** the bug consistently
3. **Check** recent changes
4. **Gather** relevant information

### Bug Report Template
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Environment details
- Error messages/logs
- Screenshots if applicable

## 💡 Feature Requests

### Before Requesting
1. **Search** existing issues
2. **Consider** the impact and effort
3. **Think** about implementation approach
4. **Check** if it aligns with project goals

### Feature Request Template
- Clear description of the feature
- Problem it solves
- Proposed solution
- Impact assessment
- Effort estimate

## 🤝 Community Guidelines

### Be Respectful
- Treat all contributors with respect
- Be patient with newcomers
- Provide constructive feedback
- Celebrate contributions

### Be Helpful
- Answer questions when you can
- Share knowledge and experience
- Mentor new contributors
- Document your work

### Be Professional
- Use clear, professional language
- Follow project conventions
- Respect project decisions
- Focus on the work, not the person

## 📞 Getting Help

### Resources
- **Issues**: [GitHub Issues](https://github.com/benweissbehavehealth/national-recovery-directory/issues)
- **Discussions**: [GitHub Discussions](https://github.com/benweissbehavehealth/national-recovery-directory/discussions)
- **Documentation**: [Project Wiki](https://github.com/benweissbehavehealth/national-recovery-directory/wiki)

### Questions
- Check existing issues and discussions first
- Be specific about your question
- Provide relevant context and code
- Be patient for responses

## 🏆 Recognition

### Contributors
- All contributors will be recognized in the README
- Significant contributions will be highlighted
- Contributors will be added to the project team

### Impact
- Your contributions help people find recovery resources
- You're supporting the recovery community
- You're advancing open data for public health

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

**Thank you for contributing to the National Recovery Support Services Directory!** 🏥

Your work helps make recovery resources more accessible to those who need them most. 