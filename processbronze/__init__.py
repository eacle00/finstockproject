import logging
import json
import azure.functions as func
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.dataset as ds
from datetime import datetime, timedelta
import fsspec
import uuid

# =========================
# CONFIG
# =========================

FILE_SYSTEM = "stocks"
BRONZE_PATH = f"{FILE_SYSTEM}/bronze"
METADATA_PATH = f"{FILE_SYSTEM}/metadata"
ACCOUNT_URL = "https://finstocksdata.dfs.core.windows.net/"
DEFAULT_START_DATE = "2023-01-01"

# =========================
# EXTRACT
# =========================

def extract_bronze_layer(symbol: str, start_date: str) -> pd.DataFrame:
    logging.info(
        f"Downloading {symbol} starting from {start_date}"
    )

    df = yf.download(
        tickers=symbol,
        start=start_date,
        interval="1d",
        group_by="column"
    )

    if df.empty:
        return pd.DataFrame()

    # flatten multi-index columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.reset_index(inplace=True)

    df.columns = df.columns.str.lower()

    return df

# =========================
# TRANSFORM
# =========================

def transform_bronze_layer(df: pd.DataFrame, symbol: str) -> pd.DataFrame:

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])

    df["ticker"] = symbol

    df["ingest_date"] = datetime.utcnow().strftime("%Y-%m-%d")

    return df

# =========================
# METADATA
# =========================

def get_metadata_path(symbol: str) -> str:
    return f"{METADATA_PATH}/{symbol}.json"

def read_metadata(fs, symbol: str) -> str:

    metadata_path = get_metadata_path(symbol)

    if not fs.exists(metadata_path):

        logging.info(
            f"No metadata found for {symbol}"
        )

        return DEFAULT_START_DATE

    with fs.open(metadata_path, "r") as f:

        metadata = json.load(f)

    last_processed_date = metadata.get(
        "last_processed_date",
        DEFAULT_START_DATE
    )

    logging.info(
        f"{symbol} last processed: "
        f"{last_processed_date}"
    )

    # IMPORTANT:
    # start from next day to avoid duplicates

    next_day = (
        datetime.strptime(
            last_processed_date,
            "%Y-%m-%d"
        ) + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    return next_day

def write_metadata(fs, symbol: str, last_processed_date: str):

    metadata_path = get_metadata_path(symbol)

    metadata = {
        "symbol": symbol,
        "last_processed_date": last_processed_date,
        "updated_at": datetime.utcnow().isoformat()
    }

    with fs.open(metadata_path, "w") as f:

        json.dump(
            metadata,
            f,
            indent=4
        )

    logging.info(
        f"Metadata updated for {symbol}"
    )

# =========================
# LOAD
# =========================

def load_bronze_layer(fs, df: pd.DataFrame) -> str:

    if df.empty:
        return None

    table = pa.Table.from_pandas(df)

    # add partition columns
    table = table.append_column("year", pa.array(df["date"].dt.year))

    table = table.append_column("month",pa.array(df["date"].dt.month))

    file_id = uuid.uuid4().hex

    ds.write_dataset(
        table,
        base_dir=BRONZE_PATH,
        filesystem=fs,
        format="parquet",
        partitioning=["ticker", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
        basename_template=f"part_{file_id}_{{i}}.parquet",
        max_rows_per_file=100_000,
        max_rows_per_group=100_000,
        use_threads=True
    )

    latest_processed_date = (
        df["date"]
        .max()
        .strftime("%Y-%m-%d")
    )

    logging.info(
        f"Upload successful until "
        f"{latest_processed_date}"
    )

    return latest_processed_date


# =========================
# PROCESS BRONZE LAYER
# =========================
def process_bronze_layer(fs, symbol: str, mode: str, start_date: str = None) -> int:

    logging.info(
        f"Processing {symbol} "
        f"in {mode} mode"
    )

    # ---------------------
    # DETERMINE START DATE
    # ---------------------

    if mode == "initial":

        if not start_date:

            raise ValueError(
                "start_date required "
                "for initial load"
            )

        effective_start_date = start_date

    elif mode == "incremental":

        effective_start_date = read_metadata(fs, symbol)

    else:

        raise ValueError(
            "Supported modes: "
            "initial, incremental"
        )

    # ---------------------
    # EXTRACT
    # ---------------------

    df = extract_bronze_layer(symbol, effective_start_date)

    if df.empty:
        logging.info(
            f"No new data for {symbol}"
        )

        return 0

    # ---------------------
    # TRANSFORM
    # ---------------------

    df = transform_bronze_layer(df, symbol)

    # ---------------------
    # LOAD
    # ---------------------

    latest_processed_date = load_bronze_layer(fs, df)

    # ---------------------
    # UPDATE METADATA
    # ONLY AFTER SUCCESSFUL UPLOAD
    # ---------------------

    write_metadata(fs, symbol, latest_processed_date)

    return len(df)

# =========================
# MAIN
# =========================

def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info(
        "Processing stock ingestion request"
    )

    try:

        body = req.get_json()

        mode = body.get("mode", "incremental")

        symbol = body.get("symbol")

        start_date = body.get("start_date", DEFAULT_START_DATE)

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

        # ---------------------
        # PROCESS SYMBOLS
        # ---------------------

        # for symbol in symbols:

        try:

            rows = process_bronze_layer(
                fs=fs,
                symbol=symbol,
                mode=mode,
                start_date=start_date
            )

            total_rows += rows

        except Exception as symbol_error:

            logging.error(
                f"{symbol} failed: "
                f"{str(symbol_error)}"
            )

        # ---------------------
        # RESPONSE
        # ---------------------

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "mode": mode,
                "rows_processed": total_rows,
                "symbol": symbol
            }),
            mimetype="application/json",
            status_code=200
        )

        # file_path = (
        #     "stocks/bronze/"
        #     "NVDA/2026/5/"
        #     "part_f8b8a69d7f9a4e1c843a666c553808c3_0.parquet"
        # )

        # with fs.open(file_path, "rb") as f:

        #     df = pd.read_parquet(f)

        # return func.HttpResponse(
        #     df.to_json(
        #         orient="records",
        #         date_format="iso"),
        #     mimetype="application/json",
        #     status_code=200
        # )

    except Exception as e:

        logging.error(str(e))

        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500
        )