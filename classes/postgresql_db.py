import psycopg
from pandas import DataFrame
import json
import logging
import os
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class PostgreSQL:
    """Class that abstracts interactions with a PostgreSQL database using DataFrames."""

    def __init__(self):
        try:
            with open("variables_paths.json") as j_file:
                j_data = json.load(j_file)
                try:
                    postgresql_config = j_data["postgresql_config.json"]
                except Exception as e:
                    logger.error(f"Error in variables_paths params: '{e}'")
                    raise

            with open(postgresql_config) as j_file:
                j_data = json.load(j_file)

            self.config = j_data
            self.conn = psycopg.connect(**self.config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error(e)
            raise

    def extract(self, sql_query: str) -> DataFrame:
        """
        Executes a SELECT query and returns the result as a DataFrame.

        :param sql_query: SQL query string, must be a SELECT query.
        :return: DataFrame with the query results.
        """
        try:
            if 'SELECT' not in sql_query.upper().split():
                raise ValueError('The provided sql_query is not a SELECT query.')

            self.cursor.execute(sql_query)
            data = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]

            df = DataFrame(data, columns=columns)
            logger.info(f"SELECT query '{sql_query}' executed successfully!")
            return df

        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            raise

    def load(self, data: DataFrame, table_name: str):
        """
        Inserts the data from a DataFrame into the specified table.

        :param data: DataFrame with the data to insert.
        :param table_name: Name of the target table in the database.
        """
        try:
            if data.empty:
                raise ValueError("The DataFrame is empty. No data to insert.")

            columns = data.columns
            headers_str = ', '.join(columns)
            values_str = ', '.join(['%s'] * len(columns))

            sql_query = f"INSERT INTO {table_name} ({headers_str}) VALUES ({values_str})"

            for _, row in data.iterrows():
                self.cursor.execute(sql_query, tuple(row))

            self.conn.commit()
            logger.info(f"Data successfully inserted into {table_name}!")
            return f"Data successfully inserted into {table_name}!"

        except Exception as e:
            logger.error(f"Error during insertion: {e}")
            raise
