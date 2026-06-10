import logging
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import uuid


class BronzeLoader:

    def __init__(self, fs, bronze_path):
        self.fs = fs
        self.bronze_path = bronze_path

    def load(self, df: pd.DataFrame) -> str:

        table = pa.Table.from_pandas(df)

        # add partition columns
        table = table.append_column("year", pa.array(df["date"].dt.year))

        table = table.append_column("month", pa.array(df["date"].dt.month))

        file_id = uuid.uuid4().hex

        ds.write_dataset(
            table,
            base_dir=self.bronze_path,
            filesystem=self.fs,
            format="parquet",
            partitioning=["ticker", "year", "month"],
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part_{file_id}_{{i}}.parquet",
            max_rows_per_file=100_000,
            max_rows_per_group=100_000,
            use_threads=True
        )

        latest_processed_date = df["date"].max().strftime("%Y-%m-%d")

        logging.info(f"Upload successful until {latest_processed_date}")

        return latest_processed_date
