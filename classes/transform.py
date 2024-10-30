import re
import logging
import os
from pandas import DataFrame
from logging import config

import main

MAIN_PATH = os.path.dirname(main.__file__)
config.fileConfig(os.path.join(MAIN_PATH, 'logging.conf'))
logger = logging.getLogger(__name__)

DEFAULT_TRANSFORM_TYPES = ['remove', 'only', 'match_regex']
FK_TRANSFORM_TYPES = ['remove', 'only']  # Types that require 'filter_keys'
REGX_TRANSFORM_TYPES = ['match_regex']  # Types that require 'str_regex'


class Transform:
    """
    Class that abstracts implemented features for default data transformation using DataFrames.
    """

    def __init__(self, data: DataFrame, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.data = data

        # Validate transform_type
        self.transform_type = kwargs.get("transform_type")
        if not self.transform_type or self.transform_type not in DEFAULT_TRANSFORM_TYPES:
            raise ValueError(
                f"The provided 'transform_type' is not accepted. Only {DEFAULT_TRANSFORM_TYPES} are accepted."
            )

        # Validate filter_keys if required
        if self.transform_type in FK_TRANSFORM_TYPES:
            self.filter_keys = kwargs.get("filter_keys")
            if not self.filter_keys:
                raise ValueError(f"'{self.transform_type}' requires 'filter_keys' to be provided.")

        # Validate str_regex if required
        if self.transform_type in REGX_TRANSFORM_TYPES:
            self.str_regex = kwargs.get("str_regex")
            if not self.str_regex:
                raise ValueError(f"'{self.transform_type}' requires 'str_regex' to be provided.")

    def apply(self) -> DataFrame:
        """
        Apply the transformation to the DataFrame based on the provided transform type.

        :return: Transformed DataFrame.
        """
        try:
            match self.transform_type:
                case "remove":
                    transformed_data = self.data.applymap(
                        lambda x: "" if x in self.filter_keys else x
                    )

                case "only":
                    transformed_data = self.data.applymap(
                        lambda x: x if x in self.filter_keys else ""
                    )

                case "match_regex":
                    regex = re.compile(self.str_regex)
                    transformed_data = self.data.applymap(
                        lambda x: x if regex.search(str(x)) else ""
                    )

            logger.info(f"Filter '{self.transform_type}' applied to the DataFrame.")
            return transformed_data

        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            raise
