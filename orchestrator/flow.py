import logging
import os
from logging import config
from classes.txt import TXT
from classes.transform import Transform
from classes.csv import CSV

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def execute(list_pipe_config: list):
    """
    Gets list with pipiline nodes in order, and execute each step of pipeline.

    :param list_pipe_config: list with dict nodes sorted by "flow" string in config file.
    """
    def get_first_data_in_cached_data(data_type: str) -> list:
        """
        Gets first data stored in data_cache for provided data_type, and remove it from cache.

        :param data_type: "extracted" or "transformed"
        :return: a data catched.
        """
        data = None

        for index in range(0, len(data_cache)):
            if data_type in data_cache[index].keys():
                data = data_cache.pop(index)[data_type]
                break

        return data

    data_cache = []
    logger.info(f"Starting Pipeline...")
    try:
        for node in list_pipe_config:
            node_name = list(node)[0]
            node_class = node[node_name]['class']
            node_type = node[node_name]['type']
            node_params = node[node_name]['params']

            logger.info(f"Starting '{node_name}' step...")
            match node_class:
                case 'extract':
                    match node_type:
                        case 'txt':
                            data_cache.append({"extracted": TXT(**node_params).extract()})
                        case _:
                            raise Exception(f"The node type '{node_type}' is not supported in extract class.")

                case 'transform':
                    tmp_extr_data = get_first_data_in_cached_data(data_type="extracted")
                    if tmp_extr_data is None:
                        raise Exception("No extracted data found to transform!")

                    match node_type:
                        case 'default':
                            data_cache.append({"transformed": Transform(data=tmp_extr_data, **node_params).apply()})
                        case _:
                            raise Exception(f"The node type '{node_type}' is not supported in extract class.")

                case 'load':
                    tmp_extr_data = get_first_data_in_cached_data(data_type="transformed")
                    if tmp_extr_data is None:
                        raise Exception("No extracted data found to transform!")

                    match node_type:
                        case 'csv':
                            data_cache.append({"loaded": CSV(data=tmp_extr_data, **node_params).load()})

                case _:
                    raise Exception(f"The node class '{node_class}' is not supported in instantiator.")

            logger.info(f"The '{node_name}' step is done!")

    except Exception as e:
        logger.error(e)
        raise

    logger.info("Pipeline Fineshed!")
    return data_cache
