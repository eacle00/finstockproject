import logging
from metadata_repo import MetadataRepository
from silver_processing.extract_silver import SilverExtractor
from silver_processing.transform_silver import SilverTransformer

class SilverProcessor:

    def __init__(self, symbol, mode, account_url, start_date, bronze_metadata_path, bronze_data_path):
        self.symbol = symbol
        self.mode = mode
        self.account_url = account_url
        self.start_date = start_date
        self.bronze_metadata_path = bronze_metadata_path
        self.bronze_data_path = bronze_data_path
    
    def process(self):
        logging.info(f"Silver: Processing {self.symbol} in {self.mode} mode")

        metadata = MetadataRepository(
            metadata_path=self.bronze_metadata_path,
            default_start_date=self.start_date,
            account_url=self.account_url
        )

        fs = metadata.create_filesystem()

        extractor = SilverExtractor()

        df = extractor.extract(fs,
                               symbol=self.symbol,
                               bronze_data_path=self.bronze_data_path
            )

        if df.empty:
            logging.info(f"No new data for {self.symbol}")

            return 0
        
        transformer = SilverTransformer()

        df = transformer.transform(df=df)
        
        return len(df)
    
    def run(self):

        total_rows = 0

        try:
            rows = self.process()

            total_rows += rows
        
        except Exception as symbol_error:

            logging.error(
                f"{self.symbol} failed: "
                f"{str(symbol_error)}"
            )
        
        return total_rows