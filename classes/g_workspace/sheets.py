import logging
import os
from logging import config
from pandas import DataFrame
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)

VALUE_INPUT_OPTION = "USER_ENTERED"


class Sheets:
    """
    Class responsible for abstracting communication with Google Sheets using DataFrames.
    """
    def __init__(self, creds, **kwargs):
        self.creds = creds
        try:
            self.sheet_id = kwargs.get('sheet_id')
            if not self.sheet_id:
                raise ValueError("To instantiate a Sheets object, the 'sheet_id' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

    def load(self, data: DataFrame, **kwargs):
        """
        Load data from a DataFrame into the Google Sheet.

        :param update_range: String with the update range. Ex: 'Sheet1!A:AZ'
        :param data: DataFrame with the data to be added to the spreadsheet.
        """
        try:
            update_range = kwargs.get('update_range')
            if not update_range:
                raise ValueError("The 'update_range' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        try:
            # Convert DataFrame to list of lists
            data_values = [data.columns.tolist()] + data.values.tolist()

            service = build('sheets', 'v4', credentials=self.creds)
            body = {'values': data_values}

            result = service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=update_range,
                valueInputOption=VALUE_INPUT_OPTION,
                body=body
            ).execute()

            logger.info(f"{result.get('updatedCells')} cells updated with data: {data.columns.tolist()}.")
            return result

        except HttpError as error:
            logger.error(f"An error occurred: {error}")
            raise

    def extract(self, **kwargs) -> DataFrame:
        """
        Extract data from the Google Sheet and return it as a DataFrame.

        :param extract_range: String with the data range to extract. Ex: 'Sheet1!A:AZ'
        :return: DataFrame with the extracted data.
        """
        try:
            extract_range = kwargs.get('extract_range')
            if not extract_range:
                raise ValueError("The 'extract_range' must be provided.")
        except Exception as e:
            logger.error(f"Error in params: '{e}'")
            raise

        try:
            service = build('sheets', 'v4', credentials=self.creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=self.sheet_id,
                range=extract_range
            ).execute()

            values = result.get('values', [])
            if not values:
                logger.warning('No data found.')
                return DataFrame()  # Return an empty DataFrame if no data is found

            # Convert the list of lists to a DataFrame
            df = DataFrame(values[1:], columns=values[0])

            logger.info(f"Data extracted from {extract_range}!")
            return df

        except HttpError as err:
            logger.error(f"An error occurred: {err}")
            raise
