import pandas as pd
from datetime import datetime, UTC

class BronzeTransformer:
    
    def transform(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])

        df["ticker"] = symbol

        df["ingest_date"] = (datetime.now(UTC).strftime("%Y-%m-%d"))

        return df