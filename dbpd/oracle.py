from .dbpd import BaseDBPD, Optional


class Oracle(BaseDBPD):
    """
    Using a SQLAlchemy session, it will connect to an Oracle database, methods can then be used from BaseDBPD.

    The SQLAlchemy connection string is automatically generated from the init arguments.

    :param username: The username of the database
    :param password: The password of the database
    :param host: The host location of the database
    :param sid: The site identifier of the database
    :param port: The port at which the database can be located
    :param threaded: Should the connection session be threaded
    :param description: A description of the database (for reference utility only)
    :param show_description: Whether the description should be printed to the console upon init
    :param sqlalchemy_create_engine_kwargs: Any keyword arguments that should be passed to the sqlalchemy.create_engine() function
    """

    def __init__(
            self,
            username: str,
            password: str,
            host: str,
            sid: str = 'prod',
            port: Optional[int] = None,
            threaded: bool = True,
            fernet_encryption_key: Optional[bytes] = None,
            description: str = 'Oracle database connection with credentials',
            show_description: bool = True,
            **sqlalchemy_create_engine_kwargs
    ):
        if port is None:
            connection_credentials = f'oracle://{username}:{password}@{host}/{sid}'
        else:
            connection_credentials = f'oracle://{username}:{password}@{host}:{port}/{sid}'
        if threaded is True:
            connection_credentials += '?threaded=true'
        elif threaded is False:
            connection_credentials += '?threaded=false'

        super(Oracle, self).__init__(
            database_type='oracle',
            connection_credentials=connection_credentials,
            fernet_encryption_key=fernet_encryption_key,
            description=description,
            show_description=show_description,
            **sqlalchemy_create_engine_kwargs
        )