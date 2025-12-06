"""
Exception types for connector operations.
"""


class ConnectorException(Exception):
    """Base exception for all connector errors."""

    pass


class RateLimitException(ConnectorException):
    """Raised when API rate limit is exceeded."""

    pass


class AuthenticationException(ConnectorException):
    """Raised when authentication fails."""

    pass


class NotFoundException(ConnectorException):
    """Raised when a resource is not found."""

    pass


class PaginationException(ConnectorException):
    """Raised when pagination fails."""

    pass


class APIException(ConnectorException):
    """Raised when API returns an error."""

    pass
