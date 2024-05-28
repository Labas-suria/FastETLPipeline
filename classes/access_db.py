import pyodbc
import logging
import os
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)


class Access:
    """Class that abstracts interactions with a Microsoft Access database."""

    def __init__(self, **kwargs):
        """:param db_file_path: Path to the file with the database."""

        try:
            if 'db_file_path' in kwargs:
                db_file_path = kwargs['db_file_path']
            else:
                raise Exception("To instantiate a Access object, the 'db_file_path' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        self.db_file_path = db_file_path
        self.conn = pyodbc.connect(
            r'Driver={Microsoft Access Driver (*.mdb, *.accdb)}' + f';DBQ={db_file_path};')
        self.cursor = self.conn.cursor()

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
                raise Exception("To load data in Access DB, the 'table_name' must be provided.")
            if 'headers' in kwargs:
                headers = kwargs['headers']
            else:
                raise Exception("To load data in Access DB, the 'headers' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        headers_str = str(headers).replace('[', '').replace(']', '').replace("'", "")
        for row in data:
            values = row.copy()
            values_str = ''

            for index in range(0, len(row)):
                if index < len(row) - 1:
                    values_str += r'?, '
                else:
                    values_str += r'?'

            sql_str = fr"insert into {table_name} ({headers_str}) values ({values_str});"
            self.cursor.execute(sql_str, tuple(values))
            self.conn.commit()
            logger.info(f"Successful insert: {sql_str}")

    def extract(self, **kwargs) -> list:
        """
        Executes a SELECT query on the given database.

        :param sql_query: String with the SQL query, must be of type SELECT.

        :return: A list of lists containing the query columns.
        """

        try:
            if 'sql_query' in kwargs:
                sql_query = kwargs['sql_query']
            else:
                raise Exception("To extract data from Access, the 'sql_query' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        if 'SELECT' not in sql_query.upper().split(' '):
            e = Exception('The provided sql_query is not a SELECT query.')
            logger.error(e)
            raise e

        list_return = []

        try:
            self.cursor.execute(sql_query)
            for tpl in self.cursor.fetchall():
                list_return.append(list(tpl))

            logger.info(f"SELECT query {sql_query} done successfully!")
            return list_return

        except Exception as e:
            logger.error(e)
            raise
