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
CORS(app, resources={r"/*": {"origins": "*"}})

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://YOUR_DATABASE_URL_HERE").replace("postgres://", "postgresql://")
DEX_API_URL = "https://api.dexscreener.com/latest/dex/tokens/solana"  # Solana-specific endpoint
DEX_API_KEY = os.getenv("DEX_API_KEY", "YOUR_API_KEY_HERE")

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
    response = requests.get(DEX_API_URL)
    response.raise_for_status()
    data = response.json()
    if not data.get("pairs"):
        raise ValueError("No data available for Solana tokens.")
    return data["pairs"]

# Filter and process data
def filter_data(raw_data):
    """Apply filters to the raw Solana data."""
    df = pd.DataFrame(raw_data)
    # Extract relevant fields with error handling
    df["price"] = pd.to_numeric(df["priceUsd"], errors="coerce")
    df["liquidity"] = pd.to_numeric(df["liquidity"].apply(lambda x: x.get("usd", 0) if x else 0), errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"].apply(lambda x: x.get("h24", 0) if x else 0), errors="coerce")
    df["created_at"] = pd.to_datetime(datetime.utcnow())
    
    # Filter tokens with liquidity > 5000
    df = df[(df["liquidity"] > 5000)]
    return df[["pairAddress", "price", "liquidity", "volume", "created_at"]]

# Save filtered data to the database
def save_to_database(df):
    """Save the filtered data to the database."""
    with engine.connect() as conn:
        for _, row in df.iterrows():
            insert_query = text("""
            INSERT INTO tokens (pair_address, price, liquidity, volume, created_at)
            VALUES (:pairAddress, :price, :liquidity, :volume, :created_at)
            """)
            conn.execute(insert_query, **row)

# API route: Fetch tokens
@app.route('/tokens', methods=['GET'])
def get_tokens():
    """Fetch and return filtered Solana tokens."""
    try:
        raw_data = fetch_data_from_dex()
        logging.info(f"Raw API Data: {raw_data}")  # Debugging
        filtered_data = filter_data(raw_data)
        save_to_database(filtered_data)  # Save to database
        return jsonify(filtered_data.to_dict(orient="records"))
    except requests.exceptions.RequestException as api_error:
        logging.error(f"API Error: {api_error}")
        return jsonify({"error": "Failed to fetch data from DEX Screener API."}), 500
    except ValueError as val_error:
        logging.error(f"Data Processing Error: {val_error}")
        return jsonify({"error": "No valid data available for Solana tokens."}), 500
    except Exception as e:
        logging.error(f"Unexpected Error: {e}")
        return jsonify({"error": "An unexpected error occurred."}), 500

# API route: Health check
@app.route('/')
def health_check():
    return "API is running successfully."

# Initialize the database when the app starts
if __name__ == '__main__':
    initialize_database()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))