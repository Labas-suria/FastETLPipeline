import logging
import os
from logging import config
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)

VALUE_INPUT_OPTION = "USER_ENTERED"


class Sheets:
    """
    Class responsible for abstracting communication with Google Sheets.
    """
    def __init__(self, creds, **kwargs):
        self.creds = creds
        try:
            if 'sheet_id' in kwargs:
                self.sheet_id = kwargs['sheet_id']
            else:
                raise Exception("To instantiate a Sheets object, the 'sheet_id' int must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

    def load(self, data: list, **kwargs):
        """
        Method responsible for load data into the spreadsheet.

        :param sheet_id: It is a string with the sheet id. The sheet_id is a string contained in the url of the
        page. Ex: https://docs.google.com/spreadsheets/d/{sheet_id}/blablablabla
        :param update_range: String with the data update interval. Ex: 'Page!A:AZ'
        :param data: List of data that will be added to the spreadsheet.
        """
        try:
            if 'update_range' in kwargs:
                update_range = kwargs['update_range']
            else:
                raise Exception("To extract data from Sheets, the 'extract_range' int must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        try:
            service = build('sheets', 'v4', credentials=self.creds)
            body = {
                'values': data
            }
            result = service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id, range=update_range,
                valueInputOption=VALUE_INPUT_OPTION, body=body).execute()
            logger.info(f"{result.get('updatedCells')} celulas atualizadas para os campos: {data[0]}.")
            return result
        except HttpError as error:
            logger.error(f"An error occurred: {error}")
            raise

    def extract(self, **kwargs) -> list:
        """
        Method responsible for extract data from the spreadsheet.

        :param extract_range: String with the data capture range. Ex: 'Page!A:AZ'

        :return: A list of lists with the data for each captured row.
        """
        try:
            if 'extract_range' in kwargs:
                extract_range = kwargs['extract_range']
            else:
                raise Exception("To extract data from Sheets, the 'extract_range' int must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        data = []
        try:
            service = build('sheets', 'v4', credentials=self.creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.sheet_id,
                                        range=extract_range).execute()
            values = result.get('values', [])
            if not values:
                logger.error('No data found.')
                return
            logger.info(f"Data extracted from {extract_range}!")
            data = values
        except HttpError as err:
            logger.error(err)
        return data
