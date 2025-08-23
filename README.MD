# Dockerized Stock Data Pipeline with Airflow

A complete, production-ready data pipeline that automatically fetches, processes, and stores stock market data using Apache Airflow orchestration and Docker containerization.

## Features

- **Automated Data Collection**: Fetches stock data from Alpha Vantage API on scheduled intervals
- **Dockerized Environment**: Full containerization with Docker Compose for easy deployment
- **PostgreSQL Integration**: Structured data storage with proper schema design
- **Robust Error Handling**: Comprehensive error management for API failures and data issues
- **Security First**: Sensitive information protected through environment variables
- **Scalable Architecture**: Designed to handle increased data volume and frequency

## Project Structure
stock-data-pipeline/
├── docker-compose.yml # Multi-container Docker setup
├── Dockerfile # Custom Airflow image configuration
├── dags/
│ └── stock_pipeline.py # Airflow DAG definition and orchestration
├── scripts/
│ └── fetch_stock_data.py # Data fetching and processing logic
├── requirements.txt # Python dependencies
├── .env.example # Environment variables template
├── .gitignore # Protects sensitive files and data
└── README.md # Project documentation

## Quick Start

### Prerequisites
- Docker
- Docker Compose
- Alpha Vantage API key (free tier available)

### Images (in the images folder)
a. Airflow Dashboard
b. Database Results

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/sanjanasrangadhol811/stock-data-pipeline.git
   cd stock-data-pipeline

2. Configure environment variables
   cp .env.example .env
# Edit .env file with your actual values

3. Build and deploy
   docker-compose up -d --build

4. Access the dashboard
  a. Airflow UI: http://localhost:8080
  b. Credentials: admin / admin
  c. PostgreSQL: localhost:5432

5. 🔧 Configuration
Environment Variables (.env)
# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
DB_CONNECTION=postgresql://airflow:airflow@postgres:5432/airflow

# API Configuration
STOCK_API_KEY=your_alpha_vantage_api_key_here
STOCK_SYMBOL=IBM

6. API Setup
a. Get a free API key from Alpha Vantage
b. Replace your_alpha_vantage_api_key_here with your actual key in the .env file

7. Data Pipeline Workflow

a. Schedule Trigger: Airflow DAG runs hourly (configurable)
b. API Data Fetch: Retrieves JSON stock data from Alpha Vantage
c. Data Processing: Parses and transforms JSON response
d. Database Storage: Upserts data into PostgreSQL table
e. Error Handling: Manages API limits and missing data gracefully

8. Database Schema
CREATE TABLE stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC(10, 4),
    high NUMERIC(10, 4),
    low NUMERIC(10, 4),
    close NUMERIC(10, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

9. Technical Stack

a.Orchestration: Apache Airflow 2.8.1
b. Database: PostgreSQL 13
c. Containerization: Docker + Docker Compose
d. Programming Language: Python 3.8
e. API Integration: Alpha Vantage REST API
f. Data Processing: Pandas, Requests, Psycopg2

10. Monitoring & Troubleshooting
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver

11. Check Database
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM stock_data;"

12. Common Issues
a. API rate limits (5 requests/minute free tier)
b. Network connectivity to Alpha Vantage
c. Database connection settings

13. Scaling & Production Considerations
a. Implement API rate limit handling with retry logic
b. Add data validation and quality checks
c. Set up monitoring and alerting
d. Implement backup and recovery strategies
e. Add data partitioning for large datasets


