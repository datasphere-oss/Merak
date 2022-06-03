class MerakSignal(BaseException):
    """
    Signals inherit from `BaseException` and will not be caught by normal error
    handling. This allows us to bypass typical error handling by raising signals.

    See `merak.engine.signals` for additional subclasses used for raising state
    transitions.

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class VersionLockMismatchSignal(merakSignal):
    """
    Raised when version locking is enabled and a task run state version sent to Cloud
    does not match the version expected by the server.

    This is not backwards compatible with `merak.utilities.exceptions.VersionLockError`

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class TaskTimeoutSignal(merakSignal):
    """
    Raised when a task reaches a timeout limit

    This is not backwards compatible with `merak.utilities.exceptions.TaskTimeoutError`

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class MerakException(Exception):
    """
    The base exception type for all merak related exceptions

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class ClientError(MerakException):
    """
    Raised when there is error in merak Client <-> Server communication

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class ObjectNotFoundError(ClientError):
    """
    Raised when an object is not found on the Server

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class AuthorizationError(ClientError):
    """
    Raised when there is an issue authorizing with merak Cloud

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)


class FlowStorageError(MerakException):
    """
    Raised when there is an error loading a flow from storage

    Args:
        - message: A message with additional information about the error
    """

    def __init__(self, message: str = "") -> None:
        super().__init__(message)
