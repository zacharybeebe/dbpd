from .dbpd import BaseDBPD, Optional, sqlite3


class SQLite(BaseDBPD):
    """
    Will connect to (or create) a SQLite database, methods can then be used from BaseDBPD

    :param filepath: The filepath of the SQLite database (or where the new one should be created)
    :param description: A description of the database (for reference utility only)
    :param show_description: Whether the description should be printed to the console upon init
    :param sqlite_connection_kwargs: Any keyword arguments that should be passed to the sqlite3.connect() function
    """

    def __init__(
            self,
            filepath: str,
            fernet_encryption_key: Optional[bytes] = None,
            description: str = 'SQLite database connection',
            show_description: bool = True,
            **sqlite_connection_kwargs
    ):
        super(SQLite, self).__init__(
            database_type='sqlite',
            filepath=filepath,
            fernet_encryption_key=fernet_encryption_key,
            description=description,
            show_description=show_description,
            **sqlite_connection_kwargs
        )


class SQLiteInMemory(BaseDBPD):
    """
    Will create an in-memory SQLite database, methods can then be used from BaseDBPD
    The first argument for sqlite3.connect() will be ':memory:'

    Use the save_as() method to save the in-memory database to disk

    :param description: A description of the database (for reference utility only)
    :param show_description: Whether the description should be printed to the console upon init
    :param sqlite_connection_kwargs: Any keyword arguments that should be passed to the sqlite3.connect() function
    """

    def __init__(
            self,
            fernet_encryption_key: Optional[bytes] = None,
            description: str = 'In-Memory SQLite database connection',
            show_description: bool = True,
            **sqlite_connection_kwargs
    ):
        super(SQLiteInMemory, self).__init__(
            database_type='sqlite',
            sqlite_in_memory=True,
            fernet_encryption_key=fernet_encryption_key,
            description=description,
            show_description=show_description,
            **sqlite_connection_kwargs
        )

    def save_as(self, filepath: str, return_new_database_manager: bool = False) -> Optional[SQLite]:
        """
        Saves an in-memory SQLite database to a SQLite database file

        :param filepath: The filepath of the new SQLite database
        :param return_new_database_manager: Boolean indicating if the method should return a dbpd.SQLite object of the new database
        :return: Optional[dbpd.SQLite]
        """
        if not filepath.endswith('.db') and not filepath.endswith('.sqlite'):
            filepath += '.db'
        new_database = sqlite3.connect(filepath)
        with new_database:
            self.db_conn.backup(new_database)
        new_database.close()

        if return_new_database_manager:
            return SQLite(filepath=filepath)