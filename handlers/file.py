import json


def get_json_from_file(file_path: str) -> dict:
    """
    Gets data from .json file and returns as a dictionary.

    :param file_path: path to .json file.

    :return: .json data as a dictionary.
    """
    try:
        with open(file_path) as file:
            json_data = json.load(file)
    except FileNotFoundError as e:
        print(f"Json file not Found: {e}")
        raise e
    except Exception as e:
        print(f"Error: {e}")
        json_data = {}

    return json_data
