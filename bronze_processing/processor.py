import fsspec
from config import Config

class ProcessorBronze:
    def create_filesystem(self):

        fs = fsspec.filesystem(
            "abfs",
            account_name=Config.ACCOUNT_URL.replace("https://", "").split(".")[0],
            anon=False
        )
    