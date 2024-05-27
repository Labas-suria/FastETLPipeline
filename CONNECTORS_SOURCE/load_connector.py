from abstract_connectors.interfaces import AbstractLoad


class LoadClass(AbstractLoad):
    def __init__(self, data: list, **kwargs):
        super().__init__(data=data, **kwargs)

    def load(self):
        return "false/path.txt"
