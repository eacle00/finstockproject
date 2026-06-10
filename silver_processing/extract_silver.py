import logging
import pandas as pd
import pyarrow.dataset as ds

class SilverExtractor:
    def extract(self, fs, symbol: str, bronze_data_path: str) -> pd.DataFrame:
        logging.info(f"Extracting {symbol} from bronze layer")

        dataset = ds.dataset(
            f"{bronze_data_path}/{symbol}",
            filesystem=fs,
            format="parquet"
        )
        
        df = dataset.to_table().to_pandas()
        df['ticker'] = symbol

        logging.info(f"Done extracting {symbol} data for silver layer")
        
        return df