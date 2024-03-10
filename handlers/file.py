import json
import logging
import os
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


def get_json_from_file(file_path: str) -> dict:
    """
    Gets data from .json file and returns as a dictionary.

    :param file_path: path to .json file.

    :return: .json data as a dictionary.
    """
    try:
        with open(file_path) as file:
            json_data = json.load(file)
    except FileNotFoundError as e:
        logger.error(f"Json file not Found: {e}")
        raise e
    except Exception as e:
        logger.error(e)
        json_data = {}

    return json_data
