import os
import logging
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
import requests
import pandas as pd
from datetime import datetime, timedelta

# Flask app setup
app = Flask(__name__)

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "YOUR_DATABASE_URL_HERE").replace("postgres://", "postgresql://")
DEX_API_URL = "https://api.dexscreener.io/latest/dex/tokens"  # Replace with the actual API URL
DEX_API_KEY = os.getenv("DEX_API_KEY", "YOUR_API_KEY_HERE")

# Connect to the database
engine = create_engine(DATABASE_URL)

# Initialize database
def initialize_database():
    """Create the tokens table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tokens (
        id SERIAL PRIMARY KEY,
        token_name VARCHAR(255),
        price FLOAT,
        liquidity FLOAT,
        volume FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        logging.info("Database initialized.")

# Fetch data from DEX API
def fetch_data_from_dex():
    """Fetch data from the DEX API."""
    headers = {"Authorization": f"Bearer {DEX_API_KEY}"}
    response = requests.get(DEX_API_URL, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data["tokens"]

# Filter and process data
def filter_data(raw_data):
    """Apply filters to the raw data."""
    df = pd.DataFrame(raw_data)
    # Convert strings to numeric
    df["price"] = pd.to_numeric(df["price"])
    df["liquidity"] = pd.to_numeric(df["liquidity"])
    df["volume"] = pd.to_numeric(df["volume"])
    
    # Filter tokens with liquidity > 5000 and created within the last 3 days
    min_date = datetime.utcnow() - timedelta(days=3)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df = df[(df["liquidity"] > 5000) & (df["created_at"] >= min_date)]
    return df

# API route: Fetch tokens
@app.route('/tokens', methods=['GET'])
def get_tokens():
    """Fetch and return filtered tokens."""
    try:
        raw_data = fetch_data_from_dex()
        filtered_data = filter_data(raw_data)
        return jsonify(filtered_data.to_dict(orient="records"))
    except Exception as e:
        logging.error(f"Error fetching tokens: {e}")
        return jsonify({"error": str(e)}), 500

# API route: Health check
@app.route('/')
def health_check():
    return "API is running successfully."

# Initialize the database when the app starts
if __name__ == '__main__':
    initialize_database()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
