from abstract_connectors.interfaces import AbstractTransform, AbstractExtract


class MergeConector(AbstractExtract):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        return ["extraído com o connector!"]
