import logging
import pandas as pd
import yfinance as yf


class BronzeExtractor:
    def extract(self, symbol: str, start_date: str) -> pd.DataFrame:
        logging.info(f"Extracting {symbol} starting from {start_date}")

        df = yf.download(
            tickers=symbol,
            start=start_date,
            interval="1d",
            group_by="column"
        )

        if df.empty:
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.reset_index(inplace=True)

        df.columns = df.columns.str.lower()

        logging.info(f"Done extracting data for {symbol}")

        return df
