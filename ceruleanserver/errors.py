class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class MissingProductError(Error):
    """Raised when a product is assumed to exist that doesn't.

    Attributes:
        product_id -- The product that is sought
        message -- Human readable explanation of the error
    """

    def __init__(self, product_id, message):
        self.product_id = product_id
        self.message = message
