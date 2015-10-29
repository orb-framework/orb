import orb


class SingleTransaction(object):
    """
    This context will automatically close connections to a given set of databases.
    If no databases are provided, then all databases in the system will be used.
    """
    def __init__(self, *databases):
        self._databases = databases or orb.system.databases()

    def __enter__(self):
        # nothing needed here
        pass

    def __exit__(self, exc_type, error, traceback):
        for db in self._databases:
            db.disconnect()
