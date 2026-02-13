# Global Remote Tech + Quant Job Scraper

An automated job aggregator that scrapes remote-friendly tech and quant roles and displays them on a GitHub Pages website.

![Jobs](https://img.shields.io/badge/dynamic/json?color=success&label=Total%20Jobs&query=length&url=https://Mbuguamaureen01.github.io/Anywhere-jobs-fetcher/jobs.json)
![Workflow](https://img.shields.io/github/actions/workflow/status/Mbuguamaureen01/Anywhere-jobs-fetcher/fetch-jobs.yml?label=Scraper)

## Features

- Multi-source scraping
- Remote/global filter with Kenya allowance (remote, worldwide, work from anywhere, or Kenya)
- Tech + quant focus (SWE, data, MLE, quant, trading systems)
- Automated updates via GitHub Actions
- CSV and JSON exports
- Deduplication
- GitHub Pages hosting

## Supported Sources

| Source | Method | Status |
|--------|--------|--------|
| SEEK (AU) | Web Scraping | Active |
| Adzuna | Official API | Optional (requires API key) |
| LinkedIn | Public Search | Optional |
| GradConnection (AU) | Web Scraping | Active |
| AusJobs (GitHub) | Curated list | Active |
| RemoteOK | Public API | Active |
| Remotive | Public API | Active |
| WeWorkRemotely | RSS | Active |

## Quick Setup

1. Fork this repository.
2. Enable GitHub Pages (Settings -> Pages -> Source: GitHub Actions).
3. Enable GitHub Actions.
4. Optional: add Adzuna API keys in repo secrets:
   - ADZUNA_APP_ID
   - ADZUNA_APP_KEY
5. Run the workflow: Actions -> Fetch Global Jobs -> Run workflow.
6. Visit: https://YOUR_USERNAME.github.io/Anywhere-jobs-fetcher/

## Configuration

- Role keywords: edit JOB_KEYWORDS in scraper.py
- Remote filter terms: edit REMOTE_KEYWORDS in scraper.py
- Kenya terms: edit KENYA_KEYWORDS in scraper.py
- Schedule: edit .github/workflows/fetch-jobs.yml

## Output Files

- jobs.csv
- jobs.json
- index.html

## Local Development

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/Anywhere-jobs-fetcher.git
cd Anywhere-jobs-fetcher

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the scraper
python scraper.py --merge
```

## Notes

- If Adzuna keys are not set, the scraper skips Adzuna and still runs.
- Remote filtering is keyword-based, so ensure REMOTE_KEYWORDS and KENYA_KEYWORDS are tuned.

## License

MIT
