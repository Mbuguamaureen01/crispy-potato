#!/usr/bin/env python3
"""
Global Tech + Quant Job Scraper
Fetches direct job postings across target countries and role keywords.
"""

import os
import csv
import json
import hashlib
import re
import sys
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse, parse_qs
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

try:
    MAX_JOB_AGE_HOURS = int(os.environ.get("MAX_JOB_AGE_HOURS", "24"))
except ValueError:
    MAX_JOB_AGE_HOURS = 24

OPEN_CHECK_ENABLED = os.environ.get("OPEN_CHECK_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")

try:
    OPEN_CHECK_TIMEOUT = int(os.environ.get("OPEN_CHECK_TIMEOUT", "6"))
except ValueError:
    OPEN_CHECK_TIMEOUT = 6

OUTPUT_COLUMNS = [
    'id',
    'title',
    'company',
    'location',
    'salary',
    'url',
    'source',
    'posted_date',
    'scraped_at',
]

# Job title keywords - must match at least one
JOB_KEYWORDS = [
    'software engineer', 'software developer', 'swe', 'sde',
    'data engineer', 'data developer', 'data analyst', 'data analytics',
    'data scientist', 'machine learning', 'ml engineer', 'ai engineer',
    'backend', 'back-end', 'frontend', 'front-end', 'fullstack', 'full-stack', 'full stack',
    'devops', 'sre', 'site reliability', 'platform engineer', 'cloud engineer',
    'bi developer', 'business intelligence', 'data reporting', 'reporting analyst',
    'python developer', 'java developer', 'javascript developer', 'typescript',
    'web developer', 'mobile developer', 'ios developer', 'android developer',
    'database', 'dba', 'etl', 'data warehouse', 'analytics engineer',
    'graduate', 'intern', 'junior', 'entry level',
    'solutions engineer', 'technical analyst', 'systems engineer',
    'quant', 'quantitative', 'quant developer', 'quant engineer',
    'quantitative developer', 'quantitative analyst', 'quant researcher',
    'quant trader', 'algorithmic trading', 'algo trading', 'trading systems',
    'mle', 'machine learning engineer', 'applied scientist',
]

# Remote/global keywords - must match at least one
REMOTE_KEYWORDS = [
    'remote',
    'fully remote',
    'work from anywhere',
    'work from home',
    'wfh',
    'anywhere',
    'global',
    'worldwide',
    'distributed',
]

# Kenya keywords for relaxed filter
KENYA_KEYWORDS = [
    'kenya',
    'nairobi',
    'mombasa',
    'kisumu',
    'nakuru',
]

def generate_job_id(title, company, url):
    """Generate unique ID for deduplication."""
    return hashlib.md5(f"{title}|{company}|{url}".encode()).hexdigest()[:12]

def clean_text(text):
    """Clean whitespace from text."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def parse_datetime_utc(value):
    """Parse supported datetime/date strings into UTC datetime."""
    if not value:
        return None

    raw = clean_text(value)
    if not raw:
        return None

    # ISO datetimes, optionally with Z suffix
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # RFC-822 style strings (RSS)
    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Date-only fallback
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None

def is_recent_post(posted_dt, max_age_hours):
    """Check if a posting falls within the recency window."""
    if posted_dt is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    return posted_dt >= cutoff

def is_likely_open_job_url(url):
    """Lightweight check for obviously dead/closed job links."""
    if not url or not str(url).startswith("http"):
        return False

    closed_markers = [
        "/closed",
        "/expired",
        "job-not-found",
        "no-longer-available",
        "position-filled",
        "position-closed",
    ]

    lower_url = str(url).lower()
    if any(marker in lower_url for marker in closed_markers):
        return False

    try:
        resp = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=OPEN_CHECK_TIMEOUT)
        final_url = (resp.url or url).lower()
        if any(marker in final_url for marker in closed_markers):
            return False
        if resp.status_code in (404, 410, 451):
            return False
        if resp.status_code in (405,):
            resp = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=OPEN_CHECK_TIMEOUT, stream=True)
            final_url = (resp.url or url).lower()
            if any(marker in final_url for marker in closed_markers):
                return False
            if resp.status_code in (404, 410, 451):
                return False
    except Exception:
        # Do not drop links on transient network issues.
        return True

    return True

def is_valid_job_title(title):
    """Check if title matches our target job keywords."""
    if not title:
        return False
    title_lower = title.lower()
    return any(kw in title_lower for kw in JOB_KEYWORDS)

def is_acceptable_location(location, title="", url=""):
    """Allow remote/global roles or Kenya-based roles."""
    check_text = f"{location} {title} {url}".lower()
    return any(kw in check_text for kw in REMOTE_KEYWORDS) or any(kw in check_text for kw in KENYA_KEYWORDS)

def is_direct_job_url(url):
    """Check if URL is a direct job posting, not a generic careers page."""
    if not url:
        return False
    url_lower = url.lower()
    
    # Must have job identifiers in URL
    job_patterns = [
        r'/job[s]?/',
        r'/position[s]?/',
        r'/opening[s]?/',
        r'/requisition',
        r'/req\d+',
        r'/jid/',
        r'/jobid/',
        r'/job-\d+',
        r'/apply/',
        r'jobid=',
        r'job_id=',
        r'positionid=',
        r'id=\d+',
        r'/\d{5,}',  # Long numeric ID
        r'greenhouse\.io/.+/jobs/',
        r'lever\.co/.+/',
        r'workday\.com/.+/job/',
        r'myworkdayjobs\.com/.+/job/',
        r'smartrecruiters\.com/.+/\d+',
        r'seek\.com\.au/job/',
        r'indeed\.com/.+/viewjob',
        r'linkedin\.com/jobs/view/',
    ]
    
    # Generic pages to reject
    generic_patterns = [
        r'^https?://[^/]+/?$',  # Just domain
        r'/careers/?$',
        r'/jobs/?$',
        r'/about/?$',
        r'/company/?$',
        r'/teams?/?$',
        r'/culture/?$',
    ]
    
    # Reject generic pages
    for pattern in generic_patterns:
        if re.search(pattern, url_lower):
            return False
    
    # Accept if has job identifier
    for pattern in job_patterns:
        if re.search(pattern, url_lower):
            return True
    
    # Otherwise reject
    return False

def scrape_github_ausjobs():
    """Scrape the curated AusJobs GitHub repository - already vetted Australia internships."""
    print("🔍 Scraping GitHub AusJobs repository...")
    jobs = []
    
    try:
        url = "https://raw.githubusercontent.com/AusJobs/Australia-Tech-Internship/main/README.md"
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        content = response.text
        
        # Parse markdown table: | [Role](URL) | Company | Location | Notes | Date |
        pattern = r'\|\s*\[([^\]]+)\]\(([^)]+)\)\s*\|\s*([^|]+)\|\s*([^|]+)\|'
        matches = re.findall(pattern, content)
        
        for match in matches:
            title = clean_text(match[0])
            job_url = match[1].strip()
            company = clean_text(match[2])
            location = clean_text(match[3])
            
            # Skip headers
            if title.lower() in ['role', 'company', 'position'] or '---' in title:
                continue
            
            # Must be valid URL
            if not job_url.startswith('http'):
                continue
            
            # This source is curated for Australian internships.
            jobs.append({
                'id': generate_job_id(title, company, job_url),
                'title': title,
                'company': company,
                'location': location if location else 'Australia',
                'salary': '',
                'url': job_url,
                'source': 'GitHub-AusJobs',
                'posted_date': datetime.now(timezone.utc).isoformat(),
                'scraped_at': datetime.now(timezone.utc).isoformat(),
            })
    except Exception as e:
        print(f"  Error: {e}")
    
    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_seek():
    """Scrape SEEK for Australia tech + quant jobs - exact posting URLs."""
    print("🔍 Scraping SEEK Australia...")
    jobs = []
    
    # Specific searches for our target roles
    searches = [
        ("data-engineer", "data engineer"),
        ("data-analyst", "data analyst"),
        ("data-scientist", "data scientist"),
        ("software-engineer", "software engineer"),
        ("software-developer", "software developer"),
        ("backend-developer", "backend developer"),
        ("frontend-developer", "frontend developer"),
        ("full-stack-developer", "full stack developer"),
        ("devops-engineer", "devops engineer"),
        ("machine-learning-engineer", "ml engineer"),
        ("python-developer", "python developer"),
        ("graduate-software", "graduate developer"),
        ("junior-developer", "junior developer"),
        ("intern-software", "intern"),
        ("bi-developer", "bi developer"),
        ("analytics-engineer", "analytics engineer"),
        ("quantitative-analyst", "quantitative analyst"),
        ("quant-developer", "quant developer"),
        ("quant-engineer", "quant engineer"),
        ("machine-learning-engineer", "machine learning engineer"),
        ("mle", "mle"),
    ]
    
    for search_slug, search_name in searches:
        try:
            url = f"https://www.seek.com.au/{search_slug}-jobs/in-All-Australia"
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                continue
            
            soup = BeautifulSoup(response.text, 'lxml')
            articles = soup.find_all('article', {'data-card-type': 'JobCard'})
            
            for article in articles[:15]:  # Top 15 per search
                try:
                    title_elem = article.find('a', {'data-automation': 'jobTitle'})
                    if not title_elem:
                        continue
                    
                    title = clean_text(title_elem.get_text())
                    link = title_elem.get('href', '')
                    
                    # Build full SEEK job URL
                    if link and not link.startswith('http'):
                        link = f"https://www.seek.com.au{link}"
                    
                    # Verify it's a direct job URL (has /job/ in path)
                    if '/job/' not in link:
                        continue
                    
                    company_elem = article.find('a', {'data-automation': 'jobCompany'})
                    location_elem = article.find('a', {'data-automation': 'jobLocation'})
                    salary_elem = article.find('span', {'data-automation': 'jobSalary'})
                    
                    company = clean_text(company_elem.get_text()) if company_elem else ""
                    location = clean_text(location_elem.get_text()) if location_elem else ""
                    salary = clean_text(salary_elem.get_text()) if salary_elem else ""
                    
                    # Skip if no company
                    if not company:
                        continue
                    
                    # Verify remote role
                    if not is_acceptable_location(location, title, link):
                        continue
                    
                    jobs.append({
                        'id': generate_job_id(title, company, link),
                        'title': title,
                        'company': company,
                        'location': location,
                        'salary': salary,
                        'url': link,
                        'source': 'SEEK',
                        'posted_date': datetime.now(timezone.utc).isoformat(),
                        'scraped_at': datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    continue
        except Exception as e:
            continue
    
    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_adzuna_api():
    """Scrape Adzuna API for multi-country tech + quant jobs."""
    print("🔍 Checking Adzuna API...")
    jobs = []
    
    app_id = os.environ.get('ADZUNA_APP_ID', '')
    app_key = os.environ.get('ADZUNA_APP_KEY', '')
    
    if not app_id or not app_key:
        print("  Adzuna API credentials not found, skipping")
        return jobs
    
    searches = [
        "data engineer",
        "data analyst",
        "data scientist",
        "software engineer",
        "software developer",
        "devops engineer",
        "machine learning",
        "python developer",
        "graduate developer",
        "junior developer",
        "quant engineer",
        "quant developer",
        "quantitative analyst",
        "machine learning engineer",
        "mle",
    ]

    # Adzuna supports a subset of countries via country-code endpoints.
    country_endpoints = [
        ("au", "Australia"),
        ("gb", "United Kingdom"),
        ("us", "United States"),
        ("ca", "Canada"),
    ]

    for endpoint_country, location_name in country_endpoints:
        for search in searches:
            try:
                api_url = f"https://api.adzuna.com/v1/api/jobs/{endpoint_country}/search/1"
                params = {
                    'app_id': app_id,
                    'app_key': app_key,
                    'results_per_page': 25,
                    'what': search,
                    'where': location_name,
                    'sort_by': 'date',
                    'max_days_old': 14,
                }

                response = requests.get(api_url, params=params, timeout=15)
                if response.status_code != 200:
                    continue

                data = response.json()

                for result in data.get('results', []):
                    title = clean_text(result.get('title', ''))
                    company = clean_text(result.get('company', {}).get('display_name', ''))
                    location = clean_text(result.get('location', {}).get('display_name', ''))
                    link = result.get('redirect_url', '')

                    # Skip if missing essential fields
                    if not title or not company or not link:
                        continue

                    # Verify job title matches our criteria
                    if not is_valid_job_title(title):
                        continue

                    # Verify remote role
                    if not is_acceptable_location(location, title, link):
                        continue

                    salary = ""
                    if result.get('salary_min') and result.get('salary_max'):
                        salary = f"${int(result['salary_min']):,} - ${int(result['salary_max']):,}"
                    elif result.get('salary_min'):
                        salary = f"${int(result['salary_min']):,}+"

                    jobs.append({
                        'id': generate_job_id(title, company, link),
                        'title': title,
                        'company': company,
                        'location': location,
                        'salary': salary,
                        'url': link,
                        'source': f'Adzuna-{endpoint_country.upper()}',
                        'posted_date': result.get('created', '') if result.get('created') else datetime.now(timezone.utc).isoformat(),
                        'scraped_at': datetime.now(timezone.utc).isoformat(),
                    })
            except Exception:
                continue
    
    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_remoteok():
    """Scrape RemoteOK API for remote roles."""
    print("ðŸ” Scraping RemoteOK API...")
    jobs = []

    try:
        api_url = "https://remoteok.io/api"
        response = requests.get(api_url, headers=HEADERS, timeout=20)
        if response.status_code != 200:
            return jobs

        data = response.json()
        for item in data:
            if not isinstance(item, dict):
                continue
            if item.get("legal"):
                continue

            title = clean_text(item.get("position", ""))
            company = clean_text(item.get("company", ""))
            link = item.get("url", "") or item.get("apply_url", "")
            location = clean_text(item.get("location", "")) or "Remote"

            if not title or not company or not link:
                continue
            if not is_valid_job_title(title):
                continue
            if not is_acceptable_location(location, title, link):
                continue

            jobs.append({
                'id': generate_job_id(title, company, link),
                'title': title,
                'company': company,
                'location': location,
                'salary': '',
                'url': link,
                'source': 'RemoteOK',
                'posted_date': item.get("date", "") if item.get("date") else datetime.now(timezone.utc).isoformat(),
                'scraped_at': datetime.now(timezone.utc).isoformat(),
            })
    except Exception:
        pass

    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_remotive():
    """Scrape Remotive public API for remote roles."""
    print("ðŸ” Scraping Remotive API...")
    jobs = []

    try:
        api_url = "https://remotive.com/api/remote-jobs"
        response = requests.get(api_url, headers=HEADERS, timeout=20)
        if response.status_code != 200:
            return jobs

        data = response.json()
        for item in data.get("jobs", []):
            title = clean_text(item.get("title", ""))
            company = clean_text(item.get("company_name", ""))
            link = item.get("url", "")
            location = clean_text(item.get("candidate_required_location", "")) or "Remote"

            if not title or not company or not link:
                continue
            if not is_valid_job_title(title):
                continue
            if not is_acceptable_location(location, title, link):
                continue

            jobs.append({
                'id': generate_job_id(title, company, link),
                'title': title,
                'company': company,
                'location': location,
                'salary': '',
                'url': link,
                'source': 'Remotive',
                'posted_date': item.get("publication_date", "") if item.get("publication_date") else datetime.now(timezone.utc).isoformat(),
                'scraped_at': datetime.now(timezone.utc).isoformat(),
            })
    except Exception:
        pass

    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_weworkremotely():
    """Scrape We Work Remotely RSS feed for programming roles."""
    print("ðŸ” Scraping We Work Remotely RSS...")
    jobs = []

    try:
        rss_url = "https://weworkremotely.com/categories/remote-programming-jobs.rss"
        response = requests.get(rss_url, headers=HEADERS, timeout=20)
        if response.status_code != 200:
            return jobs

        root = ET.fromstring(response.text)
        items = root.findall(".//item")
        for item in items:
            title = clean_text(item.findtext("title", default=""))
            link = clean_text(item.findtext("link", default=""))
            pub_date = clean_text(item.findtext("pubDate", default=""))

            # Title format often "Company: Role"
            company = ""
            role = title
            if ":" in title:
                parts = title.split(":", 1)
                company = clean_text(parts[0])
                role = clean_text(parts[1])
            else:
                role = title

            if not role or not link:
                continue
            if not is_valid_job_title(role):
                continue
            if not is_acceptable_location("Remote", role, link):
                continue

            jobs.append({
                'id': generate_job_id(role, company, link),
                'title': role,
                'company': company if company else "WeWorkRemotely",
                'location': "Remote",
                'salary': '',
                'url': link,
                'source': 'WeWorkRemotely',
                'posted_date': pub_date if pub_date else datetime.now(timezone.utc).isoformat(),
                'scraped_at': datetime.now(timezone.utc).isoformat(),
            })
    except Exception:
        pass

    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_linkedin_public():
    """Scrape LinkedIn public job listings for global tech + quant jobs."""
    print("🔍 Scraping LinkedIn public listings...")
    jobs = []
    
    searches = [
        ("data%20engineer", "Australia"),
        ("data%20analyst", "Kenya"),
        ("software%20engineer", "United%20Arab%20Emirates"),
        ("python%20developer", "United%20Kingdom"),
        ("graduate%20software", "United%20States"),
        ("quant%20engineer", "Canada"),
        ("quant%20developer", "United%20Kingdom"),
        ("quantitative%20analyst", "United%20States"),
        ("machine%20learning%20engineer", "Kenya"),
        ("mle", "United%20Arab%20Emirates"),
    ]
    
    for search, location in searches:
        try:
            # LinkedIn public job search
            url = f"https://www.linkedin.com/jobs/search?keywords={search}&location={location}&f_TPR=r604800"
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                continue
            
            soup = BeautifulSoup(response.text, 'lxml')
            job_cards = soup.find_all('div', class_='base-card')
            
            for card in job_cards[:10]:
                try:
                    title_elem = card.find('h3', class_='base-search-card__title')
                    company_elem = card.find('h4', class_='base-search-card__subtitle')
                    location_elem = card.find('span', class_='job-search-card__location')
                    link_elem = card.find('a', class_='base-card__full-link')
                    
                    if not all([title_elem, company_elem, link_elem]):
                        continue
                    
                    title = clean_text(title_elem.get_text())
                    company = clean_text(company_elem.get_text())
                    location = clean_text(location_elem.get_text()) if location_elem else ""
                    link = link_elem.get('href', '')
                    
                    # Verify it's a direct job URL
                    if '/jobs/view/' not in link:
                        continue
                    
                    # Verify remote role
                    if not is_acceptable_location(location, title, link):
                        continue
                    
                    # Verify job title
                    if not is_valid_job_title(title):
                        continue
                    
                    jobs.append({
                        'id': generate_job_id(title, company, link),
                        'title': title,
                        'company': company,
                        'location': location,
                        'salary': '',
                        'url': link.split('?')[0],  # Clean URL
                        'source': 'LinkedIn',
                        'posted_date': datetime.now(timezone.utc).isoformat(),
                        'scraped_at': datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    continue
        except Exception:
            continue
    
    print(f"  Found {len(jobs)} jobs")
    return jobs

def scrape_gradconnection():
    """Scrape GradConnection for Australia graduate tech jobs."""
    print("🔍 Scraping GradConnection...")
    jobs = []
    
    try:
        url = "https://au.gradconnection.com/graduate-jobs/information-technology/"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            return jobs
        
        soup = BeautifulSoup(response.text, 'lxml')
        job_cards = soup.find_all('div', class_='job-card')
        
        for card in job_cards[:30]:
            try:
                title_elem = card.find('a', class_='job-title')
                company_elem = card.find('span', class_='company-name')
                location_elem = card.find('span', class_='location')
                
                if not title_elem:
                    continue
                
                title = clean_text(title_elem.get_text())
                link = title_elem.get('href', '')
                company = clean_text(company_elem.get_text()) if company_elem else ""
                location = clean_text(location_elem.get_text()) if location_elem else "Australia"
                
                if not link.startswith('http'):
                    link = f"https://au.gradconnection.com{link}"
                
                # Must be direct job URL
                if not is_direct_job_url(link) and '/graduate-jobs/' not in link:
                    continue
                
                # Verify remote role
                if not is_acceptable_location(location, title, link):
                    continue
                
                jobs.append({
                    'id': generate_job_id(title, company, link),
                    'title': title,
                    'company': company,
                    'location': location,
                    'salary': '',
                    'url': link,
                    'source': 'GradConnection',
                    'posted_date': datetime.now(timezone.utc).isoformat(),
                    'scraped_at': datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                continue
    except Exception as e:
        print(f"  Error: {e}")
    
    print(f"  Found {len(jobs)} jobs")
    return jobs

def main():
    print("=" * 60)
    print("🇦🇺 Global Tech + Quant Job Scraper")
    print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(
        "Filters: Remote/Global or Kenya | Tech/Quant roles | Direct job URLs | "
        f"Max age: {MAX_JOB_AGE_HOURS}h | Open check: {OPEN_CHECK_ENABLED}"
    )
    print("=" * 60)
    
    merge_mode = '--merge' in sys.argv
    
    all_jobs = []
    
    # Scrape all sources
    all_jobs.extend(scrape_github_ausjobs())
    all_jobs.extend(scrape_seek())
    all_jobs.extend(scrape_adzuna_api())
    all_jobs.extend(scrape_linkedin_public())
    all_jobs.extend(scrape_gradconnection())
    all_jobs.extend(scrape_remoteok())
    all_jobs.extend(scrape_remotive())
    all_jobs.extend(scrape_weworkremotely())
    
    print(f"\n📊 Total jobs scraped: {len(all_jobs)}")
    
    if not all_jobs and not (merge_mode and os.path.exists('jobs.csv')):
        print("⚠️ No jobs found!")
        return
    
    # Create DataFrame and deduplicate
    df = pd.DataFrame(all_jobs if all_jobs else [], columns=OUTPUT_COLUMNS)
    if not df.empty:
        df = df.drop_duplicates(subset=['id'], keep='first')
        df = df.sort_values('scraped_at', ascending=False)

    pre_filter_count = len(df)
    
    # Load existing and merge
    if merge_mode and os.path.exists('jobs.csv'):
        try:
            existing = pd.read_csv('jobs.csv', dtype=str, keep_default_na=False)
            df = pd.concat([df, existing], ignore_index=True)
            df = df.drop_duplicates(subset=['id'], keep='first')
            print(f"📂 Merged with existing data")
        except Exception:
            pass

    # Enforce output schema to avoid stray columns/NaN from merged legacy files.
    df = df.reindex(columns=OUTPUT_COLUMNS)
    for col in OUTPUT_COLUMNS:
        df[col] = df[col].fillna("").astype(str)
    df = df[df['id'] != ""]
    df = df.drop_duplicates(subset=['id'], keep='first')

    # Normalize datetimes and filter by recency + open-application likelihood.
    url_open_cache = {}
    dropped_old = 0
    dropped_closed = 0
    normalized_rows = []

    for row in df.to_dict(orient='records'):
        posted_dt = parse_datetime_utc(row.get('posted_date')) or parse_datetime_utc(row.get('scraped_at'))
        if not is_recent_post(posted_dt, MAX_JOB_AGE_HOURS):
            dropped_old += 1
            continue

        if OPEN_CHECK_ENABLED:
            url = row.get('url', '')
            if url not in url_open_cache:
                url_open_cache[url] = is_likely_open_job_url(url)
            if not url_open_cache[url]:
                dropped_closed += 1
                continue

        scraped_dt = parse_datetime_utc(row.get('scraped_at')) or datetime.now(timezone.utc)
        row['posted_date'] = posted_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        row['scraped_at'] = scraped_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        normalized_rows.append(row)

    df = pd.DataFrame(normalized_rows, columns=OUTPUT_COLUMNS)
    if not df.empty:
        df = df.drop_duplicates(subset=['id'], keep='first')
        df = df.sort_values('posted_date', ascending=False)

    print(
        "Filtering summary: "
        f"start={pre_filter_count}, "
        f"dropped_old={dropped_old}, "
        f"dropped_closed={dropped_closed}, "
        f"kept={len(df)}"
    )
    
    # Save CSV
    df.to_csv('jobs.csv', index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
    print(f"✅ Saved {len(df)} jobs to jobs.csv")
    
    # Save JSON
    with open('jobs.json', 'w', encoding='utf-8') as f:
        json.dump({
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'total_jobs': len(df),
            'jobs': df.to_dict(orient='records')
        }, f, indent=2, ensure_ascii=False, allow_nan=False)
    print(f"✅ Saved {len(df)} jobs to jobs.json")
    
    print("\n🏁 Done!")

if __name__ == '__main__':
    main()

