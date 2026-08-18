# NPIScan Bulk Scraper

Downloads and parses CMS NPPES weekly data files to extract healthcare provider updates. Bypasses Cloudflare entirely by using official CMS data files.

## Features

- Downloads weekly ZIP files from `download.cms.gov`
- Parses 330+ column CSV files
- Maps 500+ taxonomy codes to human-readable descriptions
- Filters by date range
- Clean CSV output

## Usage

```bash
cd npiscan_scraper
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python scraper.py
```

## Output

CSV with columns: Name, Specialty, NPI Number, City, State, Provider Type, Update Date

## Data Source

CMS NPPES weekly files: https://download.cms.gov/nppes/
