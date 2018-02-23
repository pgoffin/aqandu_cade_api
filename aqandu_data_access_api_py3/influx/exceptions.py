# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass


class UnknownIDError(Error):
    """Raised when ID is not in db"""
    pass
