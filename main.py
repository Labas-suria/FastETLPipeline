import json
import logging
import os
from logging import config

from handlers import file, configuration
from orchestrator import flow


MAIN_PATH = os.path.dirname(__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)

with open("variables_paths.json") as j_file:
    j_data = json.load(j_file)
    try:
        path_json = j_data["path_json"]
    except Exception as e:
        logger.error(f"Error in variables_paths params: '{e}'")
        raise

if __name__ == '__main__':

    raw_config_data = file.get_json_from_file(file_path=path_json)
    prep_config_data = configuration.get_pipeline_nodes(pipe_config_data=raw_config_data)
    data_cache = flow.execute(list_pipe_config=prep_config_data)

    print(data_cache)
