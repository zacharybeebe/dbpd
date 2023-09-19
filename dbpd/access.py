from .dbpd import BaseDBPD, Optional


class Access(BaseDBPD):
    """
    Will connect to (or create) an MS Access database, methods can then be used from BaseDBPD

    :param filepath: The filepath of the Access database (or where the new one should be created)
    :param description: A description of the database (for reference utility only)
    :param show_description: Whether the description should be printed to the console upon init
    :param pyodbc_connection_kwargs: Any keyword arguments that should be passed to the pyodbc.connect() function
    """

    def __init__(
            self,
            filepath: str,
            fernet_encryption_key: Optional[bytes] = None,
            description: str = 'MS Access database connection',
            show_description: bool = True,
            **pyodbc_connection_kwargs
    ):
        super(Access, self).__init__(
            database_type='access',
            filepath=filepath,
            description=description,
            show_description=show_description,
            fernet_encryption_key=fernet_encryption_key,
            **pyodbc_connection_kwargs
        )