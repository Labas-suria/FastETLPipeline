from abstract_connectors.interfaces import AbstractTransform


class TransformClass(AbstractTransform):
    def __init__(self, data: list, **kwargs):
        super().__init__(data=data, **kwargs)

    def apply(self) -> list:
        return [self.data]
