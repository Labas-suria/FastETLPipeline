import json
import os
import os.path
import logging
from logging import config
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def run() -> Credentials:
    """
    Method responsible for authenticating with the Google API using the "crendentials.json" file. Returns
    the connection credentials as a "google.oauth2.credentials.Credentials" object.

    *It is necessary to have the credentialials.json file generated with the GCP application at the root of the project.*

    >> Google API documentation: https://developers.google.com/docs/api/quickstart/python?hl=pt-br

    :return: google.oauth2.credentials.Credentials
    """
    creds = None

    with open("variables_paths.json") as json_file:
        j_data = json.load(json_file)
        try:
            token_json = j_data["token.json"]
            credentials_json = j_data["credentials.json"]
        except Exception as e:
            logger.error(f"Error in variables_paths params: '{e}'")
            raise

    if os.path.exists(token_json):
        creds = Credentials.from_authorized_user_file(token_json, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_json, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_json, 'w') as token:
            token.write(creds.to_json())
    return creds
