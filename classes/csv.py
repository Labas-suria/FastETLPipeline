import logging
import os
from pandas import DataFrame, read_csv
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class CSV:
    """
    Class that abstracts .csv data manipulation using pandas DataFrame.
    """

    def __init__(self, data: DataFrame = None, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.data = data
        if 'file_path' in kwargs:
            self.file_path = kwargs['file_path']
        else:
            e = Exception("To instantiate a CSV object, the 'file_path' to file must be provided.")
            logger.error(e)
            raise e

    def load(self) -> str:
        """
        Save the DataFrame to a CSV file.

        :return: The path to the saved file.
        """
        try:
            if self.data is None or self.data.empty:
                raise ValueError("No data available to save.")

            self.data.to_csv(self.file_path, index=False)
            logger.info(f"Output data loaded in: {str(self.file_path)}")
            return self.file_path

        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            raise

    def extract(self) -> DataFrame:
        """
        Extract data from a CSV file and return it as a DataFrame.

        :return: DataFrame with the extracted data.
        """
        try:
            df = read_csv(self.file_path)
            logger.info(f"Data extracted from: {self.file_path}")
            return df

        except Exception as e:
            logger.error(f"Error extracting CSV: {e}")
            raise
