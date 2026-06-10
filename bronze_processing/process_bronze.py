import logging
from metadata_repo import MetadataRepository
from bronze_processing.extract_bronze import BronzeExtractor
from bronze_processing.transform_bronze import BronzeTransformer
from bronze_processing.load_bronze import BronzeLoader

class BronzeProcessor:

    def __init__(self,
                 symbol,
                 mode,
                 start_date,
                 account_url,
                 bronze_data_path,
                 bronze_metadata_path):
        
        self.symbol = symbol
        self.mode = mode
        self.start_date = start_date
        self.account_url = account_url
        self.bronze_data_path = bronze_data_path
        self.bronze_metadata_path = bronze_metadata_path
    
    def process(self):

        logging.info(f"Bronze: Processing {self.symbol} in {self.mode} mode")

        metadata = MetadataRepository(
            metadata_path=self.bronze_metadata_path,
            default_start_date=self.start_date,
            account_url=self.account_url
        )

        fs = metadata.create_filesystem()

        if self.mode == "initial":

            if not self.start_date:

                raise ValueError(
                    "start_date required "
                    "for initial load"
                )

            effective_start_date = self.start_date

        elif self.mode == "incremental":

            effective_start_date = metadata.read(self.symbol)

        else:

            raise ValueError(
                "Supported modes: "
                "initial, incremental"
            )

        extractor = BronzeExtractor()

        df = extractor.extract(symbol=self.symbol, start_date=effective_start_date)

        if df.empty:
            logging.info(f"No new data for {self.symbol}")

            return 0

        transformer = BronzeTransformer()

        df = transformer.transform(df=df, symbol=self.symbol)

        loader = BronzeLoader(fs, self.bronze_data_path)

        latest_processed_date = loader.load(df)

        metadata.write(
            symbol=self.symbol,
            last_processed_date=latest_processed_date
        )

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
