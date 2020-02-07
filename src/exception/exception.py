class BaseError(Exception):
    def __init__(self ,message ,code):
        super().__init__(self)
        self._code = code
        self._message = message
