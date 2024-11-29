import logging
import os
from logging import config
from queue import Queue

from pandas import DataFrame
from classes.txt import TXT
from classes.transform import Transform
from classes.csv import CSV
from classes.g_workspace import auth
from classes.g_workspace.sheets import Sheets
from classes.mysql_db import MySQL
from classes.postgresql_db import PostgreSQL
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
        script_name = script_import_list[-1]

        imp = __import__(script_import)
        imp_class = getattr(getattr(imp, script_name), class_name)
        if not issubclass(type(imp_class(data=DataFrame(), **node_params)),
                          (AbstractTransform, AbstractExtract, AbstractLoad)):
            raise Exception("The Connector must extend: AbstractTransform, AbstractExtract, or AbstractLoad.")

    except Exception as e:
        logger.error(e)
        raise

    return imp_class


def execute(list_pipe_config: list, node_steps_list: Queue = None):
    """
    Executes the pipeline by processing each node step-by-step.

    :param node_steps_list: Queue for tracking finished nodes in the pipeline.
    :param list_pipe_config: List of nodes in order, defined by flow in the configuration.
    """

    def get_first_data_in_cached_data(data_type: str) -> DataFrame:
        """Retrieves and removes the first DataFrame of the specified type from the cache."""
        for index, cache_item in enumerate(data_cache):
            if data_type in cache_item:
                return data_cache.pop(index)[data_type]
        return DataFrame()

    data_cache = []
    logger.info(f"Starting Pipeline...")
    try:
        for node in list_pipe_config:
            node_name = list(node)[0]
            node_info = node[node_name]
            node_class = node_info['class']
            node_type = node_info['type']
            node_params = node_info['params']

            logger.info(f"Starting '{node_name}' step...")
            match node_class:
                case 'extract':
                    match node_type:
                        case 'txt':
                            data_cache.append({"extracted": TXT(**node_params).extract()})
                        case 'csv':
                            data_cache.append({"extracted": CSV(**node_params).extract()})
                        case 'connector':
                            imp_class = __conector_caller(node_params)
                            extracted_data = imp_class(**node_params).extract()
                            if not isinstance(extracted_data, DataFrame):
                                raise TypeError("The connector must return a DataFrame.")
                            data_cache.append({"extracted": extracted_data})
                        case 'g_sheets':
                            creds = auth.run()
                            data_cache.append({"extracted": Sheets(creds, **node_params).extract(**node_params)})
                        case 'mysql':
                            data_cache.append({"extracted": MySQL().extract(**node_params)})
                        case 'postgresql':
                            data_cache.append({"extracted": PostgreSQL().extract(**node_params)})
                        case _:
                            raise ValueError(f"Unsupported node type '{node_type}' for extraction.")

                case 'transform':
                    extracted_data = get_first_data_in_cached_data("extracted")
                    if extracted_data.empty:
                        extracted_data = get_first_data_in_cached_data("transformed")
                        if extracted_data.empty:
                            raise ValueError("No extracted data available for transformation.")

                    match node_type:
                        case 'default':
                            transformed_data = Transform(extracted_data, **node_params).apply()
                            data_cache.append({"transformed": transformed_data})
                        case 'connector':
                            imp_class = __conector_caller(node_params)
                            transformed_data = imp_class(extracted_data, **node_params).apply()
                            if not isinstance(transformed_data, DataFrame):
                                raise TypeError("The connector must return a DataFrame.")
                            data_cache.append({"transformed": transformed_data})
                        case 'void':
                            data_cache.append({"transformed": extracted_data})
                        case _:
                            raise ValueError(f"Unsupported node type '{node_type}' for transformation.")

                case 'load':
                    transformed_data = get_first_data_in_cached_data("transformed")
                    if transformed_data.empty:
                        raise ValueError("No transformed data available for loading.")

                    match node_type:
                        case 'csv':
                            CSV(data=transformed_data, **node_params).load()
                            data_cache.append({"loaded": transformed_data})
                        case 'connector':
                            imp_class = __conector_caller(node_params)
                            imp_class(transformed_data, **node_params).load()
                            data_cache.append({"loaded": transformed_data})
                        case 'g_sheets':
                            creds = auth.run()
                            Sheets(creds, **node_params).load(data=transformed_data, **node_params)
                            data_cache.append({"loaded": transformed_data})
                        case 'mysql':
                            MySQL().load(data=transformed_data, **node_params)
                            data_cache.append({"loaded": transformed_data})
                        case 'postgresql':
                            PostgreSQL().load(data=transformed_data, **node_params)
                            data_cache.append({"loaded": transformed_data})
                        case _:
                            raise ValueError(f"Unsupported node type '{node_type}' for loading.")

                case _:
                    raise ValueError(f"Unsupported node class '{node_class}'.")

            logger.info(f"The '{node_name}' step is complete!")
            if node_steps_list:
                node_steps_list.put(node_name)

    except KeyError as e:
        logger.error(f"KeyError in pipeline configuration: missing {e} in '{node_name}' node.")
        if node_steps_list:
            node_steps_list.put('end')
        raise
    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
        if node_steps_list:
            node_steps_list.put('end')
        raise

    logger.info("Pipeline execution finished!")
    if node_steps_list:
        node_steps_list.put('end')

    return data_cache
