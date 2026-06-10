import pandas as pd
import logging


class SilverTransformer:

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming data for the silver layer")

        # ========================
        # CLEANING & VALIDATION
        # ========================

        df = df.rename(columns={"date": "trade_date"})
        df["ingest_date"] = pd.to_datetime(df["ingest_date"])
        df = df.sort_values("trade_date")
        df = df.drop_duplicates(subset=["ticker", "trade_date"])

        df = df[
            (df["high"] >= df["low"]) &
            (df["high"] >= df["open"]) &
            (df["high"] >= df["close"]) &
            (df["low"] <= df["open"]) &
            (df["low"] <= df["close"])
        ]

        df = df[df["volume"] >= 0]

        # =======================
        # ADDING NEW FEATURES
        # =======================

        df["daily_return"] = (df.groupby("ticker")["close"].pct_change())
        df["sma_5"] = (df.groupby("ticker")["close"]
                       .transform(lambda x: x.rolling(5).mean()))
        df["sma_20"] = (df.groupby("ticker")["close"]
                        .transform(lambda x: x.rolling(20).mean()))
        df["volatility_20"] = (df.groupby("ticker")["daily_return"]
                               .transform(lambda x: x.rolling(20).std()))
        df["volume_change"] = (df.groupby("ticker")["volume"].pct_change())
        df["target_direction"] = ((df.groupby("ticker")["close"]
                                   .shift(-1) > df["close"]).astype(int))

        df.to_csv('C:/Users/AL/Desktop/Resume/test.csv', index=False)

        return df
