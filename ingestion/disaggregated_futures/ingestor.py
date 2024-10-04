import os
import zipfile
import pandas as pd
import requests
from io import BytesIO
from ingestion.util.util import load_yaml, to_snake_case


class COTIngestor:
    """
    Ingests data from the annual Commitments of Traders report to a
    local data lake on the users file system
    """

    def __init__(self, year, data_lake_path=None):
        """
        Args:
            year (int) - year of data to ingest
            data_lake_path (str) - root to local data lake. defaults to
            currrent working dir
        """
        self.data_lake_path = data_lake_path or os.getcwd()
        self.year = year
        self.setup()

    # A few properties to enfore data lake naming conventions
    @property
    def data_lake_data_path(self):
        """Root of local data lake path"""
        return os.path.join(self.data_lake_path, "data")

    def get_write_path(self):
        """Write path"""
        table_name = self.config["write"]["table_name"]
        version = self.config["write"].get("version", "v1")

        return os.path.join(self.data_lake_data_path, table_name, version) + "/"

    def setup(self):
        """
        Set up data lake structure if first time loading
        Load ingestion configuration
        """
        os.makedirs(self.data_lake_path, exist_ok=True)
        os.makedirs(self.data_lake_data_path, exist_ok=True)
        self.config = load_yaml(
            "ingestion/disaggregated_futures/ingestion_config.yml",
            config_variables={"year": self.year},
        )
        os.makedirs(self.get_write_path(), exist_ok=True)

    @staticmethod
    def download_data(url):
        """
        Download zip file from the specified URL and extract it in memory.
        """
        response = requests.get(url)
        if response.status_code == 200:
            zip_file = zipfile.ZipFile(BytesIO(response.content))
            return zip_file
        else:
            raise Exception(f"Failed to download data from {self.download_url}")

    def read_input(self):
        """
        Read the .txt file from the zip archive and load it into a DataFrame.
        """
        zip_file = self.download_data(self.config["read"]["url"])

        # Assuming there is a single .txt file in the .zip
        txt_filename = [f for f in zip_file.namelist() if f.endswith(".txt")][0]

        read_options = self.config["read"].get("read_options", {})
        with zip_file.open(txt_filename) as file:
            dataframe = pd.read_csv(file, **read_options)
        return dataframe

    def transform(self, dataframe):
        """
        Apply dataset transformations.
        """
        dataframe["year"] = self.year
        dataframe.columns = [to_snake_case(col) for col in dataframe.columns]

        return dataframe

    def write(self, dataframe):
        """
        Write the transformed DataFrame to the data lake, partitioned by year.
        """
        write_options = self.config["write"].get("write_options", {})
        dataframe.to_parquet(self.get_write_path(), **write_options)

    def run(self):
        """
        Execute the data ingestion workflow: download, transform, and write.
        """
        print("\tDownloading data...")
        dataframe = self.read_input()
        print("\tApplying data transforms...")
        dataframe = self.transform(dataframe)
        print("\tWriting data...")
        self.write(dataframe)
