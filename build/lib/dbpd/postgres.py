from .dbpd import BaseDBPD, Optional


class Postgres(BaseDBPD):
    """
    Using a SQLAlchemy session, it will connect to an Postgres database, methods can then be used from BaseDBPD.

    The SQLAlchemy connection string is automatically generated from the init arguments.

    :param username: The username of the database
    :param password: The password of the database
    :param host: The host location of the database
    :param database_name: The name of the database
    :param postgres_schema: The schema of the database tables
    :param port: The port at which the database can be located
    :param description: A description of the database (for reference utility only)
    :param show_description: Whether the description should be printed to the console upon init
    :param sqlalchemy_create_engine_kwargs: Any keyword arguments that should be passed to the sqlalchemy.create_engine() function
    """
    def __init__(
            self,
            username: str,
            password: str,
            host: str,
            database_name: str,
            postgres_schema: str = 'public',
            port: Optional[int] = None,
            fernet_encryption_key: Optional[bytes] = None,
            description: str = 'Postgres database connection with credentials',
            show_description: bool = True,
            **sqlalchemy_create_engine_kwargs
    ):
        if port is None:
            connection_credentials = f'postgresql://{username}:{password}@{host}/{database_name}'
        else:
            connection_credentials = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

        super(Postgres, self).__init__(
            database_type='postgres',
            connection_credentials=connection_credentials,
            postgres_schema=postgres_schema,
            fernet_encryption_key=fernet_encryption_key,
            description=description,
            show_description=show_description,
            **sqlalchemy_create_engine_kwargs
        )

