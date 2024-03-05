import csv


class CSV:
    """
    Class that abstract a .csv data manipulation.
    """

    def __init__(self, data: list, **kwargs):
        """kwargs = params dict in pipeline.json"""

        self.data = data
        if 'file_path' in kwargs:
            self.file_path = kwargs['file_path']
        else:
            raise Exception("To instantiate a CSV object, the 'file_path' to file must be provided.")

    def load(self):
        with open(self.file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(self.data)

        print(str(self.data) + " carregado em: " + str(self.file_path))

