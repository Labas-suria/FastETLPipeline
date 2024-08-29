import json
import logging
import os
from logging import config

import flet as ft

from handlers import file, configuration
from orchestrator import flow

from app.ui.pages.main_page import MainPage

MAIN_PATH = os.path.dirname(__file__)
PIPELINE_FILES_PATH = os.path.join(MAIN_PATH, 'PIPELINE_FILES')
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)

with open("variables_paths.json") as j_file:
    j_data = json.load(j_file)
    try:
        path_json = j_data["path_json"]
    except Exception as e:
        logger.error(f"Error in variables_paths params: '{e}'")
        raise


def main(page: ft.Page):
    main_page = MainPage(page=page, path_pipe_files=PIPELINE_FILES_PATH)
    main_page.build()


if __name__ == '__main__':
    ft.app(target=main)
