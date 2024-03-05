import re

DEFAULT_TRANSFORM_TYPES = ['remove', 'only', 'match_regex']
FK_TRANSFORM_TYPES = ['remove', 'only']  # transform_types that needs a filter_keys
REGX_TRANSFORM_TYPES = ['match_regex']  # transform_types that needs a str_regex


class Transform:
    """
    Class that abstract an implemented features for default data transformation.
    """

    def __init__(self, data: list, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.data = data
        if "transform_type" in kwargs:
            if kwargs["transform_type"] not in DEFAULT_TRANSFORM_TYPES:
                raise Exception(f"The provided 'transform_type' is not accepted. Only {DEFAULT_TRANSFORM_TYPES} "
                                f"are accepted to Transform object.")

            self.transform_type = kwargs["transform_type"]
        else:
            raise Exception("To instantiate a Transform object, the 'transform_type' must be provided.")

        if self.transform_type in FK_TRANSFORM_TYPES:
            if "filter_keys" in kwargs:
                self.filter_keys = kwargs["filter_keys"]
            else:
                raise Exception(f"To {FK_TRANSFORM_TYPES} transform_types, filter_keys must be provided!")

        if self.transform_type in REGX_TRANSFORM_TYPES:
            if "str_regex" in kwargs:
                self.str_regex = kwargs["str_regex"]
            else:
                raise Exception(f"To {REGX_TRANSFORM_TYPES} transform_types, str_regex must be provided!")

    def apply(self) -> list:

        match self.transform_type:
            case "remove":
                tmp_data = []
                for line in self.data:
                    tmp_line = []
                    for item in line:
                        if item in self.filter_keys:
                            tmp_line.append("")
                        else:
                            tmp_line.append(item)
                    tmp_data.append(tmp_line)

            case "only":
                tmp_data = []
                for line in self.data:
                    tmp_line = []
                    for item in line:
                        if item not in self.filter_keys:
                            tmp_line.append("")
                        else:
                            tmp_line.append(item)
                    tmp_data.append(tmp_line)

            case "match_regex":
                tmp_data = []
                for line in self.data:
                    tmp_line = []
                    for item in line:
                        item_match = re.search(self.str_regex, item)
                        if item_match:
                            tmp_line.append(item)
                        else:
                            tmp_line.append("")
                    tmp_data.append(tmp_line)
        return tmp_data
