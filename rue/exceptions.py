"""
All exceptions inherit from RueError.
"""

class RueError(Exception): pass
class WriteError(RueError): pass

class DatabaseSetupFailure(RueError):
    """
    Subclasses of this exception are raised during setup().
    """

class DatabaseNotReady(RueError):
    """
    Raised in check() when the database is not ready for use.
    """

class DatabaseVersionTooLow(DatabaseSetupFailure):
    """
    Raised when the database version is lower than rue's schema version, and rue does
    not know how to migrate it.
    """

class DatabaseVersionTooHigh(DatabaseSetupFailure):
    """
    Raised when the database version is higher than rue's schema version.
    """

class NoItemServes(RueError):
    pass

class LostTheRace(RueError):
    """
    An invalid state error. This is not a database consistency error; it is likely that
    someone else simply beat us to the punch, or you used an outdated Entry object.
    Note that in some cases, if the new state is identical to what we were trying to do,
    rue will pretend like it succeeded.
    """
    pass
