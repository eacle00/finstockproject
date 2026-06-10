from bronze_processing.transform_bronze import BronzeTransformer
from bronze_processing.extract_bronze import BronzeExtractor
import pandas as pd
from datetime import datetime, UTC

def test_bronze_transform():
    symbol = 'AAPL'
    effective_start_date = '2026-06-05'
    
    extractor = BronzeExtractor()
    transformer = BronzeTransformer()

    raw = extractor.extract(symbol=symbol, start_date=effective_start_date)

    source = raw.copy()

    source["date"] = pd.to_datetime(source["date"])

    source["ticker"] = symbol

    source["ingest_date"] = (datetime.now(UTC).strftime("%Y-%m-%d"))

    target = transformer.transform(df=raw, symbol=symbol)

    assert source == target