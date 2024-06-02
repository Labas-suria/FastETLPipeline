import re

from abstract_connectors.interfaces import AbstractExtract

SPLIT_INDEX = [7, 18, 49, 57]


class ScrapConnector(AbstractExtract):
    def __init__(self, **kwargs):
        self.input_file = kwargs["input_file"]

    def extract(self) -> list:

        lines_list = []

        with open(self.input_file) as file:
            for line in file:
                head_match = re.search(r"^Produto Red", line)
                line_match = re.search(r"^\d{3}-\d{3}", line)
                if line_match or head_match:
                    line = line.replace('\n', '')
                    item = ''
                    item1 = ''
                    item2 = ''
                    item3 = ''
                    item4 = ''
                    indx_word = 0

                    for charac in line:

                        if 0 <= indx_word < 7:
                            item += charac
                        if 7 < indx_word < 18:
                            item1 += charac
                        if 18 < indx_word < 49:
                            item2 += charac
                        if 49 < indx_word < 57:
                            item3 += charac
                        if 57 < indx_word < len(line)-1:
                            item4 += charac

                        indx_word += 1

                    lines_list.append([item, item1, item2, item3, item4])

        return lines_list
