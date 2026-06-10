import json
import fsspec
from datetime import datetime, timedelta, UTC
import logging


class MetadataRepository:

    def __init__(self, metadata_path, default_start_date, account_url):
        self.metadata_path = metadata_path
        self.default_start_date = default_start_date
        self.account_url = account_url

    def create_filesystem(self):

        acct_name = self.account_url.replace("https://", "").split(".")[0]
        fs = fsspec.filesystem(
            "abfs",
            account_name=acct_name,
            anon=False
        )

        return fs

    def get_path(self, symbol: str) -> str:
        return (f"{self.metadata_path}/{symbol}.json")

    def read(self, symbol: str) -> str:
        path = self.get_path(symbol)
        fs = self.create_filesystem()

        if not fs.exists(path):
            return self.default_start_date

        with fs.open(path, "r") as f:
            metadata = json.load(f)

        last_date = metadata.get("last_processed_date",
                                 self.default_start_date)

        return (datetime.strptime(last_date, "%Y-%m-%d") +
                timedelta(days=1)).strftime("%Y-%m-%d")

    def write(self, symbol: str, last_processed_date: str):

        path = self.get_path(symbol)
        fs = self.create_filesystem()

        with fs.open(path, "w") as f:

            json.dump(
                {
                    "symbol": symbol,
                    "last_processed_date": last_processed_date,
                    "updated_at": datetime.now(UTC).isoformat()
                },
                f,
                indent=4
            )

    def read_processed_files(self, silver_metapath: str) -> list:

        logging.info("Read processed files in progress")

    def get_processed_files(self, symbol: str, silver_metapath: str) -> list:
        fs = self.create_filesystem()
        files = f"{self.metadata_path}/{symbol}/**/*.parquet"
        # bronze_parquet_files = fs.glob(files)
        
        
