import os
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import logging
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Create a connection to the PostgreSQL database"""
    try:
        # Use these connection parameters instead of DB_CONNECTION
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            port="5432"
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def create_stock_table_if_not_exists():
    """Create the stock_data table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        open NUMERIC(10, 4),
        high NUMERIC(10, 4),
        low NUMERIC(10, 4),
        close NUMERIC(10, 4),
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Stock table created or already exists")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def fetch_stock_data():
    """Fetch stock data from Alpha Vantage API"""
    api_key = os.getenv('STOCK_API_KEY')
    symbol = os.getenv('STOCK_SYMBOL', 'IBM')
    
    if not api_key:
        raise ValueError("API key not found in environment variables")
    
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            raise ValueError(f"API Error: {data['Error Message']}")
        if "Note" in data:
            logger.warning(f"API Note: {data['Note']}")
        
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except ValueError as e:
        logger.error(f"JSON parsing failed: {e}")
        raise

def parse_and_store_data(data):
    """Parse JSON response and store data in PostgreSQL"""
    symbol = os.getenv('STOCK_SYMBOL', 'IBM')
    
    # Extract time series data
    time_series = data.get('Time Series (5min)', {})
    if not time_series:
        logger.warning("No time series data found in API response")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    inserted_count = 0
    
    insert_query = """
    INSERT INTO stock_data (symbol, timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    
    for timestamp, values in time_series.items():
        try:
            # Parse the data
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            open_price = float(values.get('1. open', 0))
            high_price = float(values.get('2. high', 0))
            low_price = float(values.get('3. low', 0))
            close_price = float(values.get('4. close', 0))
            volume = int(values.get('5. volume', 0))
            
            # Insert into database
            cursor.execute(insert_query, (
                symbol, dt, open_price, high_price, low_price, close_price, volume
            ))
            inserted_count += 1
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse data for timestamp {timestamp}: {e}")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Successfully inserted {inserted_count} records")
    return inserted_count

def main():
    """Main function to fetch and store stock data"""
    try:
        # Create table if it doesn't exist
        create_stock_table_if_not_exists()
        
        # Fetch data from API
        data = fetch_stock_data()
        
        # Parse and store data
        count = parse_and_store_data(data)
        
        if count > 0:
            logger.info(f"Pipeline completed successfully. Inserted {count} records.")
        else:
            logger.warning("Pipeline completed but no new records were inserted.")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()