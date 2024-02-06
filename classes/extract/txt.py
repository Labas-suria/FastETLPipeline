class TXT:
    """
    Class that abstract a .txt data extraction.
    """
    def __init__(self, **kwargs):
        """kwargs = params dict in pipeline.json"""
        try:
            if 'path' in kwargs:
                self.path = kwargs['path']
            if 'header_line' in kwargs:
                self.header_line = kwargs['header_line']
            if 'first_line' in kwargs:
                self.first_line = kwargs['first_line']
            if 'last_line' in kwargs:
                self.last_line = kwargs['last_line']
            if 'separator' in kwargs:
                self.separator = kwargs['separator']
        except Exception as e:
            print(f"Error in params: '{e}'")
