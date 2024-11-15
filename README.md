# Data Analyzer

A flexible data analysis tool for processing and analyzing large CSV datasets with unknown structures.

## Features

- Dynamic CSV import with automatic structure detection
- Data analysis and structure inspection
- Database integration for persistent storage
- Flexible query interface

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

```python
from data_analyzer import DataAnalyzer

# Initialize the analyzer
analyzer = DataAnalyzer()

# Read a CSV file
df = analyzer.read_csv("your_data.csv")

# Analyze the structure
analysis = analyzer.analyze_structure(df)

# Save to database
analyzer.save_to_db(df, "table_name")

# Query the data
results = analyzer.query_data("SELECT * FROM table_name LIMIT 5")
```

## Requirements

- Python 3.7+
- pandas
- SQLAlchemy
- numpy
- python-dotenv

## License

MIT
