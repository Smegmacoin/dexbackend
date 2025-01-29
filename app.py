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
DEX_API_BASE_URL = "https://api.dexscreener.io/latest/dex/tokens"
DEX_API_KEY = os.getenv("DEX_API_KEY", "YOUR_API_KEY_HERE")  # Optional, if required

# Connect to the database
engine = create_engine(DATABASE_URL)

# Initialize database
def initialize_database():
    """Create the tokens table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tokens (
        id SERIAL PRIMARY KEY,
        chain VARCHAR(255),
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
def fetch_data_from_dex(chain_id):
    """Fetch data from the DEX API for a specific chain."""
    url = f"{DEX_API_BASE_URL}/{chain_id}"  # Chain-specific endpoint
    headers = {"Authorization": f"Bearer {DEX_API_KEY}"}  # Add API key if required
    response = requests.get(url, headers=headers)
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
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df[(df["liquidity"] > 5000) & (df["created_at"] >= min_date)]
    return df

# Store filtered data into the database
def store_data_in_db(df, chain):
    """Store filtered tokens into the database."""
    if not df.empty:
        df["chain"] = chain  # Add chain information
        with engine.connect() as conn:
            df.to_sql("tokens", conn, if_exists="append", index=False)
        logging.info(f"{len(df)} tokens stored in the database.")

# API route: Fetch tokens by chain
@app.route('/tokens/<chain_id>', methods=['GET'])
def get_tokens_by_chain(chain_id):
    """Fetch and return filtered tokens for a specific chain."""
    try:
        raw_data = fetch_data_from_dex(chain_id)
        filtered_data = filter_data(raw_data)
        store_data_in_db(filtered_data, chain_id)
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