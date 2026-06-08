import os

class Config:
    BRONZE_PATH = os.getenv("BRONZE_PATH")
    BRONZE_METADATA_PATH = os.getenv("BRONZE_METADATA_PATH")
    ACCOUNT_URL = os.getenv("ACCOUNT_URL")
    DEFAULT_START_DATE = os.getenv("DEFAULT_START_DATE")
    DEFAULT_MODE = os.getenv("DEFAULT_MODE")