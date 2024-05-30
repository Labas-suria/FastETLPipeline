import logging
import os
from logging import config

from classes.txt import TXT
from classes.transform import Transform
from classes.csv import CSV
from classes.g_workspace import auth
from classes.g_workspace.sheets import Sheets
from classes.mysql_db import MySQL
from abstract_connectors.interfaces import AbstractTransform, AbstractExtract, AbstractLoad

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def __conector_caller(node_params: dict):
    try:
        script_import = node_params['script_import']
        class_name = node_params['class_name']
        script_import_list = script_import.split(".")
        script_name = script_import_list[len(script_import_list) - 1]

        imp = __import__(script_import)
        imp_class = getattr(getattr(imp, script_name), class_name)
        if type(imp_class(data=None, **node_params)).__base__ not in [AbstractTransform, AbstractExtract, AbstractLoad]:
            raise Exception("The Conector must extends: AbstractTransform, AbstractExtract or AbstractLoad classes")

    except Exception as e:
        logger.error(e)
        raise

    return imp_class


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
                        case 'connector':
                            if 'script_import' not in node_params.keys() or 'class_name' not in node_params.keys():
                                raise Exception("The 'script_import' and 'class_name' params must be sourced!")
                            imp_class = __conector_caller(node_params=node_params)
                            extrct_data = imp_class(**node_params).extract()

                            if type(extrct_data) is not list:
                                raise Exception("The connector returned data must be a list!")

                            data_cache.append({"extracted": extrct_data})

                        case 'g_sheets':
                            creds = auth.run()
                            data_cache.append({"extracted": Sheets(creds=creds, **node_params).extract(**node_params)})

                        case 'mysql':
                            data_cache.append({"extracted": MySQL().extract(**node_params)})

                        case _:
                            raise Exception(f"The node type '{node_type}' is not supported in extract class.")
                case 'transform':
                    tmp_extr_data = get_first_data_in_cached_data(data_type="extracted")
                    if tmp_extr_data is None:
                        raise Exception("No extracted data found to transform!")

                    match node_type:
                        case 'default':
                            data_cache.append({"transformed": Transform(data=tmp_extr_data, **node_params).apply()})
                        case 'connector':
                            if 'script_import' not in node_params.keys() or 'class_name' not in node_params.keys():
                                raise Exception("The 'script_import' and 'class_name' params must be sourced!")
                            imp_class = __conector_caller(node_params=node_params)
                            transf_data = imp_class(data=tmp_extr_data, **node_params).apply()

                            if type(transf_data) is not list:
                                raise Exception("The connector returned data must be a list!")

                            data_cache.append({"transformed": transf_data})

                        case 'void':
                            data_cache.append({"transformed": tmp_extr_data})

                        case _:
                            raise Exception(f"The node type '{node_type}' is not supported in extract class.")

                case 'load':
                    tmp_extr_data = get_first_data_in_cached_data(data_type="transformed")
                    if tmp_extr_data is None:
                        raise Exception("No transformed data found to load!")

                    match node_type:
                        case 'csv':
                            data_cache.append({"loaded": CSV(data=tmp_extr_data, **node_params).load()})

                        case 'connector':
                            if 'script_import' not in node_params.keys() or 'class_name' not in node_params.keys():
                                raise Exception("The 'script_import' and 'class_name' params must be sourced!")
                            imp_class = __conector_caller(node_params=node_params)
                            transf_data = imp_class(data=tmp_extr_data, **node_params).load()

                            data_cache.append({"loaded": transf_data})

                        case 'g_sheets':
                            creds = auth.run()
                            data_cache.append({"loaded": Sheets(creds=creds, **node_params).load(data=tmp_extr_data,
                                                                                                 **node_params)})
                        case 'mysql':
                            data_cache.append({"loaded": MySQL().load(data=tmp_extr_data, **node_params)})

                case _:
                    raise Exception(f"The node class '{node_class}' is not supported in instantiator.")

            logger.info(f"The '{node_name}' step is done!")

    except Exception as e:
        logger.error(e)
        raise

    logger.info("Pipeline Fineshed!")
    return data_cache
