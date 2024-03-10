import csv
import logging
import os
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class CSV:
    """
    Class that abstract a .csv data manipulation.
    """

    def __init__(self, data: list, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.data = data
        if 'file_path' in kwargs:
            self.file_path = kwargs['file_path']
        else:
            e = Exception("To instantiate a CSV object, the 'file_path' to file must be provided.")
            logger.error(e)
            raise e

    def load(self):
        try:
            with open(self.file_path, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(self.data)
                logger.info(f"Output data loaded in: {str(self.file_path)}")
                return self.file_path
        except Exception as e:
            logger.error(e)
            raise
