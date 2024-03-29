import logging
import os
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class TXT:
    """
    Class that abstract a .txt data manipulation.
    """
    def __init__(self, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.last_line = None
        self.first_line = 0
        self.header_line = True
        try:
            if 'path' in kwargs:
                self.path = kwargs['path']
            else:
                raise Exception("To instantiate a TXT object, the 'path' to file must be provided.")
            if 'header_line' in kwargs:
                self.header_line = kwargs['header_line']
            if 'first_line' in kwargs:
                self.first_line = kwargs['first_line']
            else:
                raise Exception("To instantiate a TXT object, the 'first_line' int must be provided.")
            if 'last_line' in kwargs:
                self.last_line = kwargs['last_line']
            if 'separator' in kwargs:
                self.separator = kwargs['separator']
            else:
                raise Exception("To instantiate a TXT object, the 'separator' int must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

    def extract(self) -> list:
        """
        Extracts data from .txt according to the node configuration provided in the "pipeline.json".

        :return: List of list with line from table.
        """

        try:
            with open(self.path) as file:
                file_lines = file.readlines().copy()

            if self.last_line is not None:
                if self.last_line > (len(file_lines)-1):
                    raise Exception("The 'last_line' is bigger then lines in file!")
                tmp_last_line = self.last_line
            else:
                tmp_last_line = (len(file_lines)-1)

            tmp_first_line = self.first_line

            if tmp_first_line >= tmp_last_line:
                raise Exception("The 'first_line' must be lower then 'last_line'!")

            headers_line = []
            lines_list = []
            if self.header_line:
                headers_line.append(file_lines[0].replace('\n', ''))

            for index in range(tmp_first_line, tmp_last_line):
                lines_list.append(file_lines[index].replace('\n', ''))

            return_list = []
            headers_line = headers_line[0].split(self.separator)
            return_list.append(headers_line)
            for line in lines_list:
                return_list.append(line.split(self.separator))

        except Exception as e:
            logger.error(e)
            raise

        logger.info(f"Data extracted from {self.path}")

        return return_list
