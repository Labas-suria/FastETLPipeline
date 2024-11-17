import logging
import os
from pandas import DataFrame
from logging import config
import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class TXT:
    """
    Class that abstracts .txt data manipulation and returns a DataFrame.
    """

    def __init__(self, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.last_line = None
        self.first_line = 0
        self.header_line = True

        try:
            self.path = kwargs.get('path')
            if not self.path:
                raise ValueError("The 'path' to file must be provided.")

            self.header_line = kwargs.get('header_line', True)
            self.first_line = kwargs.get('first_line', 0)
            self.last_line = kwargs.get('last_line')
            self.separator = kwargs.get('separator')
            if not self.separator:
                raise ValueError("The 'separator' must be provided.")

        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

    def extract(self) -> DataFrame:
        """
        Extracts data from .txt according to the configuration provided in 'pipeline.json'.

        :return: DataFrame with the extracted data.
        """
        try:
            with open(self.path, encoding='utf-8') as file:
                file_lines = file.readlines()

            tmp_last_line = self.last_line + 1 if self.last_line is not None else len(file_lines)

            if self.header_line:
                headers = file_lines.pop(0).strip().split(self.separator)
                data_start = self.first_line
            else:
                headers = None
                data_start = self.first_line

            if tmp_last_line > len(file_lines):
                raise ValueError("The 'last_line' exceeds the total number of lines in the file.")

            if self.first_line >= tmp_last_line:
                raise ValueError("The 'first_line' must be lower than 'last_line'.")

            data = [
                line.strip().split(self.separator)
                for line in file_lines[data_start:tmp_last_line]
            ]

            df = DataFrame(data, columns=headers if headers else None)

        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            raise

        logger.info(f"Data extracted from {self.path}")
        return df
