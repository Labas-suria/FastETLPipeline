import mysql.connector
from mysql.connector import errorcode

import json
import logging
import os
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class MySQL:
    """Class that abstracts interactions with a MySQL database."""
    def __init__(self):
        try:
            with open("variables_paths.json") as j_file:
                j_data = json.load(j_file)
                try:
                    mysql_config = j_data["mysql_config.json"]
                except Exception as e:
                    logger.error(f"Error in variables_paths params: '{e}'")
                    raise

            with open(mysql_config) as j_file:
                j_data = json.load(j_file)

            self.config = j_data
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Something is wrong with your user name or password")
                raise
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Database does not exist")
                raise
        except Exception as e:
            logger.error(e)
            raise

    def extract(self, **kwargs):
        """
        Executes a SELECT query on the given database.

        :param sql_query: String with the SQL query, must be of type SELECT.

        :return: A list of lists containing the query rows.
        """
        try:
            if "sql_query" in kwargs:
                sql_query = kwargs["sql_query"]
            else:
                raise Exception("To extract data from MySQL DB, the select 'sql_query' must be provided.")

            if 'SELECT' not in sql_query.upper().split(' '):
                e = Exception('The provided sql_query is not a SELECT query.')
                logger.error(e)
                raise e

            list_return = []
            self.cursor.execute(sql_query)
            for tpl in self.cursor.fetchall():
                list_return.append(list(tpl))

            logger.info(f"SELECT query {sql_query} done successfully!")
            return list_return

        except Exception as e:
            logger.error(e)
            raise

    def load(self, data: list, **kwargs):
        """
        Executes an INSERT query in the database, providing a list of headers and
        a list with the lists containing the lines with the values.

        :param table_name: Name of the table where the data will be included.
        :param headers: List with the names of the columns where the data will be inserted.
        :param data: List with lists of values to be inserted into the table. lists with values must have the
        same size with the 'headers' list.
        """
        try:
            if 'table_name' in kwargs:
                table_name = kwargs['table_name']
            else:
                raise Exception("To load data in MySQL DB, the 'table_name' must be provided.")
            if 'headers' in kwargs:
                headers = kwargs['headers']
            else:
                raise Exception("To load data in MySQL DB, the 'headers' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        headers_str = str(headers).replace('[', '').replace(']', '').replace("'", "")
        for row in data:
            values = row.copy()
            values_str = ''

            for index in range(0, len(row)):
                if index < len(row) - 1:
                    values_str += r'%s, '
                else:
                    values_str += r'%s'

            sql_str = fr"insert into {table_name} ({headers_str}) values ({values_str});"
            self.cursor.execute(sql_str, tuple(values))
            self.conn.commit()
            logger.info(f"Successfull insert: {sql_str} {tuple(values)}")

        return f"Data successfull inserted into {table_name}!"
