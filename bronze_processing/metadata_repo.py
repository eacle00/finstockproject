import json
from datetime import datetime, timedelta, UTC

class MetadataRepository:
    
    def __init__(self, fs, metadata_path, default_start_date):
        self.fs = fs
        self.metadata_path = metadata_path
        self.default_start_date = default_start_date

    def get_path(self, symbol: str) -> str:
        return (f"{self.metadata_path}/{symbol}.json")

    def read(self, symbol: str) -> str:
        path = self.get_path(symbol)

        if not self.fs.exists(path):
            return self.default_start_date

        with self.fs.open(path, "r") as f:
            metadata = json.load(f)

        last_date = metadata.get("last_processed_date", self.default_start_date)

        return (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    def write(self,symbol: str,last_processed_date: str):

        path = self.get_path(symbol)

        with self.fs.open(path, "w") as f:

            json.dump(
                {
                    "symbol": symbol,
                    "last_processed_date": last_processed_date,
                    "updated_at": datetime.now(UTC).isoformat()
                },
                f,
                indent=4
            )