import logging
import json
import fsspec
import azure.functions as func
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
FILE_SYSTEM = "stocks"
BRONZE_PATH = f"{FILE_SYSTEM}/bronze"
SILVER_PATH = f"{FILE_SYSTEM}/silver"
METADATA_PATH = f"{FILE_SYSTEM}/silver/metadata"
ACCOUNT_URL = "https://finstocksdata.dfs.core.windows.net/"


def get_metadata_path(symbol: str) -> str:
    return f"{METADATA_PATH}/{symbol}.json"

# =========================
# EXTRACT
# =========================
def extract_silver_layer():
    logging.info(f"Extracting data from the bronze layer")

# =========================
# TRANSFORM
# =========================
def transform_silver_layer():
    logging.info(f"Transforming data for the silver layer")

# =========================
# LOAD
# =========================
def load_silver_layer():
    logging.info(f"Loading data to the silver layer")

# =========================
# PROCESS SILVER LAYER
# =========================
def process_silver_layer(fs, symbol: str, mode: str):
    logging.info(f"Processing {symbol} in {mode}")

    parquet_files = fs.glob(f"{BRONZE_PATH}/**/*.parquet")
    
    print(f"Type: {type(parquet_files)}")
    for file in parquet_files:
        logging.info(file)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f"Processing the silver layer started")

    try:

        body = req.get_json()

        mode = body.get("mode", "incremental")

        symbol = body.get("symbol")

        if not symbol:
            return func.HttpResponse(
                "Missing required parameter: symbol",
                status_code=400
            )
        # ---------------------
        # ADLS FILESYSTEM
        # ---------------------

        fs = fsspec.filesystem(
            "abfs",
            account_name=ACCOUNT_URL.replace("https://", "").split(".")[0],
            anon=False
        )

        total_rows = 0

        process_silver_layer(fs=fs, symbol=symbol, mode=mode)

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "mode": mode,
                "symbol": symbol
            }),
            mimetype="application/json",
            status_code=200
        )
    
    except Exception as e:

        logging.error(str(e))

        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500
        )
