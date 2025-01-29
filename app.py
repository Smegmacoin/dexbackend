import os
import logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
import requests
import pandas as pd
from datetime import datetime, timedelta

# Flask app setup
app = Flask(__name__)

# Enable CORS for the app
CORS(app)

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://YOUR_DATABASE_URL_HERE").replace("postgres://", "postgresql://")
DEX_API_URL = "https://api.dexscreener.com/latest/dex/tokens/solana"  # Solana-specific endpoint

# Connect to the database
engine = create_engine(DATABASE_URL)

# Initialize database
def initialize_database():
    """Create the tokens table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tokens (
        id SERIAL PRIMARY KEY,
        pair_address VARCHAR(255),
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
    """Fetch data from the DEX API for Solana."""
    try:
        response = requests.get(DEX_API_URL)
        response.raise_for_status()
        data = response.json()
        if "pairs" not in data or not data["pairs"]:
            raise ValueError("No valid data available for Solana tokens.")
        return data["pairs"]
    except Exception as e:
        logging.error(f"Error fetching data from DEX: {e}")
        raise ValueError("Failed to fetch data from DEX Screener API.")

# Filter and process data
def filter_data(raw_data):
    """Apply filters to the raw Solana data."""
    df = pd.DataFrame(raw_data)
    if df.empty:
        raise ValueError("No valid token data received.")
    # Extract relevant fields
    df["price"] = pd.to_numeric(df["priceUsd"], errors="coerce")
    df["liquidity"] = pd.to_numeric(df["liquidity"].apply(lambda x: x["usd"]), errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"].apply(lambda x: x["h24"]), errors="coerce")
    df["created_at"] = pd.to_datetime(datetime.utcnow())
    
    # Filter tokens with liquidity > 5000
    df = df[(df["liquidity"] > 5000)]
    if df.empty:
        raise ValueError("No tokens met the liquidity criteria.")
    return df[["pairAddress", "price", "liquidity", "volume", "created_at"]]

# API route: Fetch tokens
@app.route('/tokens', methods=['GET'])
def get_tokens():
    """Fetch and return filtered Solana tokens."""
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