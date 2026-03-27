# Jobs Pipeline

A data pipeline that aggregates job listings from multiple sources across Kenya and beyond.

## Architecture

```
[Data Sources: LinkedIn, MyJobsInKenya, BrighterMonday, Indeed, Glassdoor, Fuzu, Adzuna API, Google Search]
   ↓
[Scrapers + APIs] (scrapers/)
   ↓
[Airflow DAGs - scheduled at 2 PM EAT daily] (dags/)
   ↓
[Data Cleaning & Transformation] (transformers/)
   ↓
[PostgreSQL Database - Aiven Cloud] (database/)
   ↓
[Grafana Monitoring Dashboard] (grafana/)
   ↓
[FastAPI Backend] (api/)
   ↓
[Frontend Web App] (coming soon)
   ↓
[Users browse & apply]
```

## Project Structure

```
jobs-pipeline/
├── api/                    # FastAPI backend
│   └── main.py
├── config/                 # Configuration
│   └── settings.py
├── dags/                   # Airflow DAGs
│   ├── daily_scrape_dag.py    # Full scrape at 2 PM EAT
│   └── quick_scrape_dag.py    # Quick scrape every 6 hours
├── database/               # DB models & connection
│   ├── connection.py
│   └── models.py
├── grafana/                # Grafana dashboards & provisioning
│   ├── dashboards/
│   └── provisioning/
├── scrapers/               # Job scrapers
│   ├── base_scraper.py
│   ├── linkedin_scraper.py
│   ├── myjobsinkenya_scraper.py
│   ├── brightermonday_scraper.py
│   ├── indeed_scraper.py
│   ├── glassdoor_scraper.py
│   ├── fuzu_scraper.py
│   ├── google_search_scraper.py
│   ├── adzuna_api_scraper.py
│   └── runner.py
├── transformers/           # Data cleaning
│   └── cleaner.py
├── .env                    # Credentials (not committed)
├── docker-compose.yml      # All services
├── Dockerfile
├── requirements.txt
└── test_connection.py      # DB connection test
```

## Quick Start


### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 1b. Initialize Airflow Database (required for first-time setup or after config changes)
**Local:**
```bash
airflow db init
```
**Render:**
Add a one-time Render shell command or deploy hook:
```bash
airflow db init
```
This must be run with Airflow version 2.8.2.

### 2. Test database connection
```bash
python test_connection.py
```

### 3. Run scrapers manually
```bash
python -m scrapers.runner
```

### 4. Start the API server
```bash
uvicorn api.main:app --reload --port 8000
```

### 5. Start everything with Docker
```bash
docker-compose up -d
```

## Services

| Service | Port | URL |
|---------|------|-----|
| Airflow UI | 8080 | http://localhost:8080 |
| Backend API | 8000 | http://localhost:8000/docs |
| Grafana | 3000 | http://localhost:3000 |

## API Endpoints

- `GET /api/jobs` - List jobs (with search, filter, pagination)
- `GET /api/jobs/{id}` - Get single job
- `GET /api/sources` - List data sources
- `GET /api/stats` - Pipeline statistics
- `POST /api/scrape/{source}` - Trigger manual scrape

## DAG Schedule

- **Daily Full Scrape**: Runs at 2:00 PM EAT (11:00 AM UTC) - scrapes all 8 sources
- **Quick Scrape**: Every 6 hours - scrapes LinkedIn, BrighterMonday, Indeed

## Data Sources

| Source | Method | Notes |
|--------|--------|-------|
| LinkedIn | Web scraping | Public job search pages |
| MyJobsInKenya | Web scraping | Kenya-focused job board |
| BrighterMonday | Web scraping | East Africa's largest job board |
| Indeed Kenya | Web scraping | Global job aggregator |
| Glassdoor | Web scraping | Jobs + company reviews |
| Fuzu | Web scraping | East Africa career platform |
| Google Search | Web scraping | Agentic search across many sites |
| Adzuna | REST API | Free tier: 250 req/month |
