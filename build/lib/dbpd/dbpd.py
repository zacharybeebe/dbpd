import datetime
import inspect
import numpy as np
import os
import pandas as pd
import pyodbc
import sqlalchemy
import sqlite3
import threading

from cryptography.fernet import Fernet
from hashlib import sha1, sha224, sha256, sha384, sha512
from sqlalchemy import text, Engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Any, Literal, Optional, Union
from uuid import uuid4
from win32com.client import Dispatch


class BaseDBPD(object):
    """
    This class is the parent class for six child classes that are specific to different database types:
        Access
        MySQL
        Oracle
        Postgres
        SQLite
        SQLiteInMemory

    Abstractions have been created such that working with these various database types
    is consistent throughout the user's code base.

    The main theme of this class is establish a working relationship between
    relational databases and Pandas DataFrames; because these objects are tabular in nature,
    it reveals itself to be an efficient way to manage and manipulate a given database

    The query() method of this class is responsible for returning SELECT sql statements as
    their respective DataFrames. However the query() method can also be used to make changes to
    the database (such as INSERT, UPDATE, DELETE, etc.)

    Users are expected to write their own sql statements to query the database, and parameterized
    queries are accepted and encouraged as well.

    This is not meant to be an Object-Relational-Mapper (ORM) and has no such functionality, although
    it may be possible for users to create their own ORM using the classes herein.

    See the README for examples of how to get started
    """

    # Exceptions
    class AccessFileError(ValueError):
        def __init__(self, filepath: str):
            msg = f'Invalid "filepath" argument: "{filepath}". Files must be ".accdb" or ".mdb" files'
            super(BaseDBPD.AccessFileError, self).__init__(msg)

    class EncryptionKeyError(AttributeError):
        def __init__(self):
            super(BaseDBPD.EncryptionKeyError, self).__init__('No "fernet_encryption_key" attribute has been set')

    class ExcelFileError(ValueError):
        def __init__(self, filepath: str):
            msg = f'Invalid "filepath" argument: "{filepath}". Files must be ".xlsx" files'
            super(BaseDBPD.ExcelFileError, self).__init__(msg)

    class InitFileError(ValueError):
        def __init__(self, filepath: str):
            msg = f'Invalid "filepath" argument: "{filepath}". Files for init must be Access or SQLite files'
            super(BaseDBPD.InitFileError, self).__init__(msg)

    class SqliteFileError(ValueError):
        def __init__(self, filepath: str):
            msg = f'Invalid "filepath" argument: "{filepath}". Files must be ".db" or ".sqlite" files'
            super(BaseDBPD.SqliteFileError, self).__init__(msg)

    @staticmethod
    def flatten_sql(sql: str) -> str:
        for i in range(1, 10):
            sql = sql.replace((' ' * (i * 4)), '')
        return sql.replace('\r', ' ').replace('\n', ' ').replace('\t', '')

    TABLE_NAME_QUERIES = {
        # Note that MS Access info will be handled by a pyodbc.connect.cursor object
        'oracle': """
            SELECT 
                table_name AS name
            FROM 
                user_tables 
            ORDER BY 
                table_name
        """,

        'postgres': """
            SELECT 
                table_schema || '.' || table_name AS name
            FROM 
                information_schema.tables 
            WHERE
                table_schema = '{schema}'
            ORDER BY 
                table_schema, 
                table_name
        """,

        'mysql': """
            SELECT 
                table_name AS name 
            FROM 
                information_schema.tables
            WHERE 
                table_schema = '{database_name}'
        """,

        'sqlite': """
            SELECT 
                name
            FROM 
                sqlite_schema 
            WHERE 
                type = 'table' 
            ORDER BY 
                name
        """
    }

    COLUMN_NAME_QUERIES = {
        # Note that MS Access info will be handled by a pyodbc.connect.cursor object
        'oracle': """
            SELECT 
                column_name AS name
            FROM 
                user_tab_columns 
            WHERE 
                table_name = '{table_name}'
            ORDER BY
                column_name
        """,

        'postgres': """
            SELECT 
                column_name AS name
            FROM 
                information_schema.columns 
            WHERE 
                table_schema || '.' || table_name = '{table_name}'
            ORDER BY
                column_name

        """,

        'mysql': """
            SELECT 
                column_name AS name
            FROM
                information_schema.columns
            WHERE 
                table_schema = '{database_name}' 
                AND table_name = '{table_name}'
        """,

        'sqlite': """
            PRAGMA table_info({table_name})
        """
    }

    def __init__(
            self,
            connection_credentials: Optional[str] = None,
            description: Optional[str] = None,
            database_type: Optional[str] = None,
            postgres_schema: Optional[str] = None,
            mysql_database_name: Optional[str] = None,
            filepath: Optional[str] = None,
            sqlite_in_memory: Optional[bool] = False,
            show_description: bool = True,
            fernet_encryption_key: Optional[bytes] = None,
            **connection_kwargs
    ):
        self.description = description
        self.database_type = database_type
        self.postgres_schema = postgres_schema
        self.mysql_database_name = mysql_database_name
        self.fernet_encryption_key = fernet_encryption_key

        self.db_conn: Union[sqlite3.Connection, pyodbc.Connection, Session]
        self.engine: Optional[Engine] = None
        self.filepath: Optional[str] = None
        self.file_ext: Optional[str] = None

        self.recent_query: Optional[str] = None
        self.recent_df: Optional[pd.DataFrame] = None

        if filepath is None:  # Start In-Memory Sqlite Database OR SQLAlchemy Engine for Postgres or Oracle
            if sqlite_in_memory:
                pass_kwargs = self._filter_callable_kwargs(func=sqlite3.connect, passed_kwargs=connection_kwargs)
                self.db_conn = sqlite3.connect(database=':memory:', **pass_kwargs)
            else:
                pass_kwargs = self._filter_callable_kwargs(func=sqlalchemy.create_engine, passed_kwargs=connection_kwargs)
                self.engine = sqlalchemy.create_engine(url=connection_credentials, **pass_kwargs)
                self.db_conn = sessionmaker(bind=self.engine)()

        else:
            if filepath.endswith('.sqlite') or filepath.endswith('.db'):
                if not os.path.isfile(filepath):
                    self._warn(f'"{os.path.basename(filepath)}" does not exist, a blank database will be created.')
                pass_kwargs = self._filter_callable_kwargs(func=sqlite3.connect, passed_kwargs=connection_kwargs)
                self.filepath = os.path.abspath(filepath).replace('\\', '/')
                self.file_ext = os.path.splitext(self.filepath)[-1]
                self.db_conn = sqlite3.connect(database=self.filepath, **pass_kwargs)

            elif filepath.endswith('.accdb') or filepath.endswith('.mdb'):
                if not os.path.isfile(filepath):
                    self._warn(f'"{os.path.basename(filepath)}" does not exist, a blank database will be created.')
                    try:
                        self.create_access_database(filepath=filepath)
                    except Exception as e:
                        fp = os.path.abspath(filepath)
                        if os.path.isfile(fp):
                            os.remove(fp)
                        raise e
                pass_kwargs = self._filter_callable_kwargs(func=pyodbc.connect, passed_kwargs=connection_kwargs)
                self.filepath = os.path.abspath(filepath).replace('\\', '/')
                self.file_ext = os.path.splitext(self.filepath)[-1]
                driver = self.access_driver(filepath=filepath)
                self.db_conn = pyodbc.connect(driver, **pass_kwargs)

            else:
                raise self.InitFileError(filepath=filepath)

        if self.description is not None and show_description:
            print(f'{self.description}\n')

    # Access and/or SQLite specific methods ##################################################################
    @staticmethod
    def _access_generate_create_table_sql_from_dataframe(dataframe: pd.DataFrame, table_name: str) -> str:
        """
        Generates the CREATE TABLE sql statement for MS Access from a given DataFrame. This private method is used
        in the export_query_to_access() method, as pandas does not have support for MS Access with the
        DataFrame's to_sql() method.

        :param dataframe: The dataframe which will represent the table schema
        :param table_name: The name of the table that should be created
        :return: str - The formatted CREATE TABLE sql statement
        """
        columns = dataframe.columns
        create_table_sql = f'CREATE TABLE {table_name} (\n\t'
        col_types = []
        for dtype, col in zip(dataframe.dtypes, columns):
            dtype_str = str(dtype)
            if dtype_str.startswith('int'):
                access_type = 'INTEGER'
            elif dtype_str.startswith('float'):
                access_type = 'DOUBLE'
            elif dtype_str.startswith('date'):
                access_type = 'DATETIME'
            elif dtype_str.startswith('bool'):
                access_type = 'BIT'
            else:
                access_type = 'VARCHAR'
            col_types.append(f'[{col}] {access_type}')
        create_table_sql += ',\n\t'.join(col_types)
        create_table_sql += '\n);'
        return create_table_sql

    def _access_columns(self, table_name: str) -> list:
        """
        Returns the column names of a given table for MS Access

        :param table_name: The name of the table
        :return: list - The column names
        """
        cursor = self.db_conn.cursor()
        cursor.execute(f'SELECT TOP 1 * FROM {table_name}')
        columns = [i[0] for i in cursor.description]
        columns.sort()
        cursor.close()
        return columns

    def _access_sqlite_query(self, sql: str, parameters: Optional[Union[dict, list]] = None) -> Optional[pd.DataFrame]:
        """
        This private method is used by the main query() method. Because Access and SQLite are not using SQLAlchemy, the
        DataFrames have to be constructed differently, if the sql query returns zero results, either it DOES return zero
        results OR the sql statement is not a SELECT statement, the return value will be None. This is consistent with
        the query() method returning None under the same circumstances.

        :param sql: The sql statement to be executed
        :param parameters: Parameters for a parameterized query
        :return: DataFrame of the query results or None
        """
        cursor = self.db_conn.cursor()
        if parameters is None:
            cursor.execute(sql)
        else:
            if self.database_type == 'sqlite':
                if isinstance(parameters, dict):
                    cursor.execute(sql, list(parameters.values()))
                else:
                    cursor.execute(sql, parameters)
            else:
                if isinstance(parameters, dict):
                    cursor.execute(sql, *list(parameters.values()))
                else:
                    cursor.execute(sql, *parameters)

        # Modify queries (UPDATE, INSERT, etc) will not have a cursor description, so return None
        if cursor.description is None:
            cursor.close()
            return None

        columns = [i[0] for i in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        if data:
            df_data = {}
            for row in data:
                for i, column in enumerate(columns):
                    if column not in df_data:
                        df_data[column] = []
                    df_data[column].append(row[i])
            return pd.DataFrame(data=df_data, columns=columns)
        else:
            return None

    def _access_tables(self) -> list:
        """
        Returns a list of all of the table names

        :return: list - A list of table names
        """
        cursor = self.db_conn.cursor()
        tables = [table_info.table_name for table_info in cursor.tables(tableType='TABLE')]
        tables.sort()
        cursor.close()
        return tables

    @classmethod
    def _create_access_new(cls, filepath: str) -> str:
        """
        Creates a new, blank MS Access database

        :param filepath: The filepath at which the database should be created
        :return: str - The filepath with forward slashes replaced with backslashes
        """
        try:
            fp = os.path.abspath(filepath).replace('/', '\\')
            access_app = Dispatch("Access.Application")
            engine = access_app.DBEngine
            workspace = engine.Workspaces(0)

            language = ';LANGID=0x0409;CP=1252;COUNTRY=0'
            create = workspace.CreateDatabase(fp, language, 64)
            return fp
        except Exception as e:
            raise e
        finally:
            access_app.DoCmd.CloseDatabase
            access_app.Quit
            create = None
            workspace = None
            engine = None
            access_app = None

    @staticmethod
    def _create_access_sqlite_database(
            connection: Union[pyodbc.connect, sqlite3.connect],
            create_table_schema_sql_list: Optional[list] = None
    ) -> None:
        """
        Can be used to create tables from a list of CREATE TABLE sql statements, used by both the public methods:
        create_access_database() and create_sqlite_database()

        :param connection: The connection object for the database
        :param create_table_schema_sql_list: The list of CREATE TABLE sql statements
        :return: None
        """
        cursor = connection.cursor()
        try:
            if create_table_schema_sql_list is not None:
                for create_table in create_table_schema_sql_list:
                    cursor.execute(create_table)
                connection.commit()
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()

    # Private Utility methods ########################################################################################################################
    @staticmethod
    def _filter_callable_kwargs(func: callable, passed_kwargs: dict) -> dict:
        """
        Filters the keyword arguments, getting only the key-value pairs that can
        actually be passed to a particular function/method ("func")

        :param func: A function/method to be inspected
        :param passed_kwargs: The keyword arguments trying to be passed to the function/method
        :return: dict - The filtered keyword arguments
        """
        return {k: v for k, v in passed_kwargs.items() if k in inspect.getfullargspec(func).args}

    def _info_query_list(
            self,
            info_type: Literal['tables', 'columns'] = 'tables',
            table_name: Optional[str] = None,  # need table_name if info_type = 'columns'
            show_names: bool = False
    ) -> list:
        """
        Used by the table_names() and column_names() public methods to abstract getting this info for various database types

        :param info_type: Either 'tables' or 'columns'
        :param table_name: If getting the column names, the name of the table needs to be passed
        :param show_names: Boolean indicating if the table or column names should be printed to the console
        :return: list - Either an empty list or a list of the table or column names
        """
        if info_type == 'tables':
            if self.database_type == 'access':
                info = self._access_tables()
                if not info:
                    info = None
            else:
                if self.postgres_schema is not None:
                    sql = self.TABLE_NAME_QUERIES[self.database_type].format(schema=self.postgres_schema)
                elif self.mysql_database_name is not None:
                    sql = self.TABLE_NAME_QUERIES[self.database_type].format(database_name=self.mysql_database_name)
                else:
                    sql = self.TABLE_NAME_QUERIES[self.database_type]
                info = self.query(sql=sql, show_head=False, warn_is_none=False)
                if info is not None:
                    info = info['name'].tolist()
        else:
            if self.database_type == 'access':
                info = self._access_columns(table_name=table_name)
                if not info:
                    info = None
            else:
                sql = self.COLUMN_NAME_QUERIES[self.database_type].format(table_name=table_name)
                info = self.query(sql=sql, show_head=False, warn_is_none=False)
                if info is not None:
                    info = info['name'].tolist()

        if info is not None:
            info = [i.lower() for i in info]
            if show_names:
                new_lines = '\n'.join(info)
                print(f'{new_lines}\n')
            return info
        else:
            return []

    @staticmethod
    def _warn(text: str) -> None:
        """
        Prints a yellow-colored warning to the console

        :param text: The warning message
        :return: None
        """
        print(f'\033[0;33;49mWARNING: {text} \033[0m' + '\n')

    # Public Utility methods #########################################################################################################################
    @classmethod
    def access_driver(cls, filepath: str) -> str:
        """
        Creates the MS Access Driver string that can be used by pyodbc.connect()

        :param filepath: The filepath of the Access database
        :return: str - The filepath with backward slashes replaced with forward slashes
        """
        if not filepath.endswith('.accdb') and not filepath.endswith('mdb'):
            raise cls.AccessFileError(filepath=filepath)
        fp = os.path.abspath(filepath).replace('\\', '/')
        return f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={fp};'

    @classmethod
    def convert_numpy_value(cls, value: Any, column: Optional[str] = None, null_zeroes_for_columns: Optional[list] = None) -> Any:
        """
        Possibly converts a numpy value (typically this value comes from a DataFrame) to its respective Python value

        :param value: The value to be possibly be converted
        :param column: The column name of the DataFrame in which the value resides - This is only used for nulling zero values
        :param null_zeroes_for_columns: A list of columns where zeros should be nullified
        :return: The converted value
        """
        if cls.isna(value):
            v = None
        elif isinstance(value, (np.int8, np.int16, np.int32, np.int64)):
            v = int(value)
        elif isinstance(value, (np.float16, np.float32, np.float64)):
            v = float(value)
        elif isinstance(value, np.bool_):
            v = True if value else False
        else:
            v = value

        if null_zeroes_for_columns is not None:
            if column is not None:
                if column in null_zeroes_for_columns and v == 0:
                    v = None
        return v

    @classmethod
    def create_access_database(cls, filepath: str, create_table_schema_sql_list: Optional[list] = None) -> str:
        """
        Creates a new MS Access database, this method can also accept a list of CREATE TABLE sql statements in which
        to define the database table schema.

        :param filepath: The filepath at which the database should be created
        :param create_table_schema_sql_list: The list of CREATE TABLE sql statements
        :return: The filepath with forward slashes replaced with backslashes
        """
        if not filepath.endswith('.accdb') and not filepath.endswith('.mdb'):
            raise cls.AccessFileError(filepath=filepath)
        try:
            fp = cls._create_access_new(filepath=filepath)
        except Exception as e:
            raise e

        driver = cls.access_driver(filepath=filepath)
        conn = pyodbc.connect(driver)
        try:
            cls._create_access_sqlite_database(connection=conn, create_table_schema_sql_list=create_table_schema_sql_list)
            return fp
        except Exception as e:
            if os.path.isfile(fp):
                os.remove(os.path.abspath(fp))
            raise e
        finally:
            conn.close()

    @classmethod
    def create_sqlite_database(cls, filepath: str, create_table_schema_sql_list: Optional[list] = None) -> str:
        """
        Creates a new SQLite database, this method can also accept a list of CREATE TABLE sql statements in which
        to define the database table schema.

        :param filepath: The filepath at which the database should be created
        :param create_table_schema_sql_list: The list of CREATE TABLE sql statements
        :return: The absolute path of the filepath
        """
        if not filepath.endswith('.db') and not filepath.endswith('.sqlite'):
            raise cls.SqliteFileError(filepath=filepath)
        fp = os.path.abspath(filepath)
        conn = sqlite3.connect(filepath)
        try:
            cls._create_access_sqlite_database(connection=conn, create_table_schema_sql_list=create_table_schema_sql_list)
            return fp
        except Exception as e:
            if os.path.isfile(fp):
                os.remove(os.path.abspath(fp))
            raise e
        finally:
            conn.close()

    def init_table_schemas_from_files(self, create_table_sql_files_list: list, commit: bool = True) -> None:
        """
        Will create tables within a BLANK database from files read in the create_table_sql_files_list. Note that
        if a database already contains tables, the method will not proceed and a warning will be printed
        to the console. For creating new tables within an existing database, use the query() method directly.

        :param create_table_sql_files_list: A list of filepaths that contain the CREATE TABLE sql statements
        :param commit: Should this transaction be committed
        :return: None
        """
        sql_read_from_files = []
        for file in create_table_sql_files_list:
            with open(file, mode='r') as f:
                sql_read_from_files.append(f.read())
        self.init_table_schemas_from_sql(create_table_sql_statements_list=sql_read_from_files, commit=commit)

    def init_table_schemas_from_sql(self, create_table_sql_statements_list: list, commit: bool = True) -> None:
        """
        Will create tables within a BLANK database CREATE TABLE sql statements in the create_table_sql_statements_list. Note that
        if a database already contains tables, the method will not proceed and a warning will be printed
        to the console. For creating new tables within an existing database, use the query() method directly.

        :param create_table_sql_statements_list: A list of the CREATE TABLE sql statements
        :param commit: Should this transaction be committed
        :return: None
        """
        if len(self.table_names(show_names=False)) > 0:
            self._warn(f'{self.__class__.__name__} already has tables, cannot initialize schema, use query() method to modify schema.')
            return None
        try:
            for file in create_table_sql_statements_list:
                with open(file, mode='r') as f:
                    self.query(sql=f.read(), show_head=False, warn_is_none=False)
                if commit:
                    self.commit()
        except Exception as e:
            self.rollback()
            raise e

    @staticmethod
    def decrypt_value_static(fernet_encryption_key: bytes, value: bytes, original_encoding: str = 'utf-8', ttl: Optional[int] = None) -> str:
        """
        This static method can be used directly if the user has an encryption key but would not like to start a database session

        :param fernet_encryption_key: The encryption key
        :param value: The value to be decrypted
        :param original_encoding: The original encoding of the value
        :param ttl: The timeout of the key
        :return: str - The string representation of the decrypted value
        """
        fernet = Fernet(fernet_encryption_key)
        return fernet.decrypt(token=value, ttl=ttl).decode(encoding=original_encoding)

    def decrypt_value(self, value: bytes, original_encoding: str = 'utf-8', ttl: Optional[int] = None) -> Any:
        """
        Users can pass a Fernet Encryption Key to the constructor of this class, possibly in the case that fields within
        the database are encrypted. Using this key, this method will decrypt a value. If no key is passed in the constructor
        (or not set after the fact) this method will raise an Exception.

        :param value: The value to be decrypted
        :param original_encoding: The original encoding of the value
        :param ttl: The timeout of the key
        :return: str - The string representation of the decrypted value
        """
        if self.fernet_encryption_key is None:
            raise self.EncryptionKeyError()
        return self.decrypt_value_static(fernet_encryption_key=self.fernet_encryption_key, value=value, original_encoding=original_encoding, ttl=ttl)

    @staticmethod
    def encrypt_value_static(fernet_encryption_key: bytes, value: Any, encoding: str = 'utf-8') -> bytes:
        """
        This static method can be used directly if the user has an encryption key but would not like to start a database session

        :param fernet_encryption_key: The encryption key
        :param value: The value to be encrypted, note that this value will be turned into a string and then bytes before encryption
        :param encoding: The encoding of the value to bytes
        :return: bytes - The encrypted value
        """
        value = bytes(str(value), encoding=encoding)
        fernet = Fernet(fernet_encryption_key)
        return fernet.encrypt(data=value)

    def encrypt_value(self, value: Any, encoding: str = 'utf-8') -> bytes:
        """
        Users can pass a Fernet Encryption Key to the constructor of this class, possibly in the case that fields within
        the database are encrypted. Using this key, this method will encrypt a value. If no key is passed in the constructor
        (or not set after the fact) this method will raise an Exception.

        :param value: The value to be encrypted, note that this value will be turned into a string and then bytes before encryption
        :param encoding: The encoding of the value to bytes
        :return: bytes - The encrypted value
        """
        if self.fernet_encryption_key is None:
            raise self.EncryptionKeyError()
        return self.encrypt_value_static(fernet_encryption_key=self.fernet_encryption_key, value=value, encoding=encoding)

    @staticmethod
    def dt_now() -> datetime.datetime.now:
        """
        Shorthand for returning a datetime.datetime.now() object
        :return: datetime.datetime.now()
        """
        return datetime.datetime.now()

    @staticmethod
    def dt_today() -> datetime.date.today:
        """
        Shorthand for returning a datetime.date.today() object
        :return: datetime.date.today()
        """
        return datetime.date.today()

    @classmethod
    def generate_fernet_encryption_key(cls) -> bytes:
        """
        Will generate a Fernet Encryption Key. Note that if values are encrypted and inserted into the database
        using this key, then this key should be stored somewhere secure as this will be the only key which
        will correctly decrypt the values.

        :return: bytes - The Fernet Encryption Key
        """
        cls._warn('Generating Fernet encryption key, make sure to save this key somewhere secure')
        return Fernet.generate_key()

    @staticmethod
    def isna(value: Any) -> bool:
        """
        Checks if a value "isna" according to pandas or if the value is None

        :param value: The value to be checked
        :return: bool
        """
        return pd.isna(value) or value is None

    @staticmethod
    def sha_hash(value: Any, sha_type: Literal['sha1', 'sha224', 'sha256', 'sha384', 'sha512'] = 'sha256', encoding: str = 'utf-8') -> str:
        """
        Will hash a value given the "sha_type" hashing algorithm. Note that the default hashing algorithm is sha256
        even if the "sha_type" argument isn't valid. This could be useful for inserting passwords into a database.

        :param value: The value to hash
        :param sha_type: The hashing algorithm to use
        :param encoding: The encoding of the value
        :return: str - The "hexdigest" string of the hash
        """
        sha_type = sha_type.lower()
        value_encoded = str(value).encode(encoding=encoding)
        if sha_type == 'sha1':
            sha = sha1
        elif sha_type == 'sha224':
            sha = sha224
        elif sha_type == 'sha384':
            sha = sha384
        elif sha_type == 'sha512':
            sha = sha512
        else:
            sha = sha256
        return sha(value_encoded).hexdigest()

    @staticmethod
    def uuid4():
        """
        Returns a universal unique identifier (UUID) version 4. Can be useful for primary key generation, if that is the desired
        primary key strategy of the database.

        :return: str - The uuid v4
        """
        return str(uuid4())

    # Transaction methods ##########################################################################################
    def close(self, commit_on_quit: bool = True) -> None:
        """
        Closes the current session with the database.

        :param commit_on_quit: Boolean indicating if a final commit should be transacted before close
        :return: None
        """
        if commit_on_quit:
            self.db_conn.commit()

        # pyodbc with MS Access can sometimes close very slowly ~15 seconds, here we just throw it in a thread to not hold anything up
        if self.database_type == 'access':
            t = threading.Thread(target=self.db_conn.close)
            t.start()
        else:
            self.db_conn.close()

        # SqlAlchemy engines will be disposed of
        if self.engine is not None:
            self.engine.dispose()

    def commit(self) -> None:
        """
        Commits current transactions to the database
        :return: None
        """
        self.db_conn.commit()

    def drop_all_tables(self, commit: bool = True) -> None:
        """
        Drops all tables from the database.

        :param commit: Boolean indicating if this dropping transaction should be committed
        :return: None
        """
        table_names = self.table_names(show_names=False)
        for table in table_names:
            self.query(sql=f'DROP TABLE IF EXISTS {table} CASCADE;', show_head=False, warn_is_none=False)
        if commit:
            self.commit()

    def drop_table(self, table_name: str, commit: bool = True):
        """
        Drops a table from the database.

        :param table_name: The table to be dropped
        :param commit: Boolean indicating if this dropping transaction should be committed
        :return: None
        """
        try:
            self.query(sql=f'DROP TABLE IF EXISTS {table_name} CASCADE;', show_head=False, warn_is_none=False)
            if commit:
                self.commit()
        except Exception as e:
            self.rollback()
            raise e

    def export_query_to_access(
            self,
            out_filepath: str,
            out_table_name: str,
            in_sql: str,
            in_parameters: Optional[Union[dict, list]] = None,
            in_show_head: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Will export query results to an MS Access database, only if the query returns results.

        :param out_filepath: The filepath of the new Access database to be created
        :param out_table_name: The name of the table that will be filled with the resulting data
        :param in_sql: The sql statement to be executed
        :param in_parameters: The parameters associated with a parameterized query
        :param in_show_head: Boolean indicating if the head of the DataFrame should print to the console
        :return: DataFrame of the result or None
        """
        if not out_filepath.endswith('.accdb') and not out_filepath.endswith('.mdb'):
            raise self.AccessFileError(filepath=out_filepath)

        df = self.query(sql=in_sql, parameters=in_parameters, show_head=in_show_head, warn_is_none=True)
        if df is None:
            self._warn('Nothing to export, MS Access database will not be created')
            return None

        if not os.path.isfile(out_filepath):
            out_filepath = self.create_access_database(filepath=out_filepath)

        db_mgr = BaseDBPD(
            database_type='access',
            filepath=out_filepath,
            show_description=False
        )

        if out_table_name not in db_mgr.table_names(show_names=False):
            create_table_sql = self._access_generate_create_table_sql_from_dataframe(dataframe=df, table_name=out_table_name)
            db_mgr.query(sql=create_table_sql, show_head=False, warn_is_none=False)
            db_mgr.commit()
        try:
            secure_cols = [f'[{i}]' for i in df.columns]
            for i in df.index:
                insert_values = [self.convert_numpy_value(value=ii) for ii in df.iloc[i].tolist()]
                # Although it may seem wise to create "place_holders" and "insert" above-outside of the for-loop because
                # the "insert_values" length will always be the same "columns" length, however it is actually
                # significantly (~7x) slower to do so, the reason is unknown to me currently, it may have to do
                # with Python memory allocation or with pyodbc itself
                place_holders = ['?' for _ in range(len(insert_values))]
                insert = f"""INSERT INTO {out_table_name} ({', '.join(secure_cols)}) VALUES ({', '.join(place_holders)});"""
                db_mgr.query(sql=insert, parameters=insert_values, show_head=False, warn_is_none=False)
            db_mgr.commit()
            return df
        except Exception as e:
            raise e
        finally:
            db_mgr.close()

    def export_query_to_excel(
            self,
            out_filepath: str,
            out_table_name: str,
            in_sql: str,
            in_parameters: Optional[Union[dict, list]] = None,
            in_show_head: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Will export query results to an MS Excel sheet, only if the query returns results.

        :param out_filepath: The filepath of the new Excel sheet to be created
        :param out_table_name: The name of the table that will be filled with the resulting data
        :param in_sql: The sql statement to be executed
        :param in_parameters: The parameters associated with a parameterized query
        :param in_show_head: Boolean indicating if the head of the DataFrame should print to the console
        :return: DataFrame of the result or None
        """
        if not out_filepath.endswith('.xlsx'):
            raise self.ExcelFileError(filepath=out_filepath)
        fp = os.path.abspath(out_filepath).replace('\\', '/')
        file_already_existed = os.path.isfile(fp)

        df = self.query(sql=in_sql, parameters=in_parameters, show_head=in_show_head, warn_is_none=True)
        if df is None:
            self._warn('Nothing to export, MS Excel sheet will not be created')
            return None
        try:
            df.to_excel(excel_writer=fp, sheet_name=out_table_name, index=False, engine='openpyxl')
            return df
        except Exception as e:
            if not file_already_existed:
                if os.path.isfile(fp):
                    os.remove(fp)
            raise e

    def export_query_to_sqlite(
            self,
            out_filepath: str,
            out_table_name: str,
            in_sql: str,
            in_parameters: Optional[Union[dict, list]] = None,
            in_show_head: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Will export query results to an SQLite database, only if the query returns results.

        :param out_filepath: The filepath of the new SQLite database to be created
        :param out_table_name: The name of the table that will be filled with the resulting data
        :param in_sql: The sql statement to be executed
        :param in_parameters: The parameters associated with a parameterized query
        :param in_show_head: Boolean indicating if the head of the DataFrame should print to the console
        :return: DataFrame of the result or None
        """
        if not out_filepath.endswith('.db') and not out_filepath.endswith('.sqlite'):
            raise self.SqliteFileError(filepath=out_filepath)
        fp = os.path.abspath(out_filepath).replace('\\', '/')
        file_already_existed = os.path.isfile(fp)

        df = self.query(sql=in_sql, parameters=in_parameters, show_head=in_show_head, warn_is_none=True)
        if df is None:
            self._warn('Nothing to export, SQLite database will not be created')
            return None

        conn = sqlite3.connect(database=fp)
        try:
            df.to_sql(name=out_table_name, con=conn, if_exists='append', index=False)
            conn.close()
            return df
        except Exception as e:
            conn.close()
            if not file_already_existed:
                if os.path.isfile(fp):
                    os.remove(fp)
            raise e

    def insert_values(self, table_name: str, **column_value_pairs) -> None:
        """
        This method can be used to insert values into a given table. The column_value_pairs keyword arguments can be used to
        set the column name with the respective value.

        :param table_name: The name of the table to insert values
        :param column_value_pairs: Keyword arguments that represent the column names and their respective values
        :return: None
        """
        allowable_columns = self.column_names(table_name=table_name, show_names=False)
        column_value_pairs = {column.lower(): value for column, value in column_value_pairs.items() if column.lower() in allowable_columns}
        columns = list(column_value_pairs.keys())
        if self.database_type in ['access', 'sqlite']:
            values = ['?' for _ in columns]
        else:
            values = [f':{column}' for column in columns]
        sql = f"""INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"""
        try:
            self.query(sql=sql, parameters=column_value_pairs, show_head=False, warn_is_none=False)
        except Exception as e:
            raise e

    def query(
            self,
            sql: str,
            parameters: Optional[Union[dict, list]] = None,
            show_head: bool = True,
            index: Optional[Union[str, list]] = None,
            warn_is_none: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Executes any user defined sql statement and if this sql statement returns data such as from a SELECT statement,
        the resulting data will be returned as a pandas DataFrame. Parameterized sql statements are accepted as well.

        Note that if a query does not return data (as in the case of a modification statement) OR the query returns zero
        results the return value will be None.

        :param sql: The sql statement to be executed
        :param parameters: The parameters associated with a parameterized query
        :param show_head: Boolean indicating if the head of the resulting DataFrame should be printed to the console
        :param index: Can be used to set the index of the resulting DataFrame
        :param warn_is_none: Boolean indicating if a warning should be printed to the console when the query returns zero results
        :return: DataFrame or None
        """
        self.recent_query = sql
        df = None
        if self.database_type in ['access', 'sqlite']:
            df = self._access_sqlite_query(sql=sql, parameters=parameters)
        else:
            sql = text(sql)
            if parameters is not None:
                executed = self.db_conn.execute(sql, parameters)
            else:
                executed = self.db_conn.execute(sql)
            if executed.returns_rows:
                try:
                    df = pd.DataFrame(data=executed.fetchall(), columns=list(executed.keys()))
                except ValueError:
                    pass  # df defaults to None

        if df is None or len(df) == 0:
            if warn_is_none:
                self._warn(f'Query returned zero results, return object will be None')
            return None
        else:
            if index is not None:
                df.set_index(index, inplace=True)
            if show_head:
                print(f'{df.head()}\n')
            self.recent_df = df
            return df

    def rollback(self) -> None:
        """
        Rolls back the database to its most recent state.

        :return: None
        """
        self.db_conn.rollback()

    # Info methods ##########################################################################################
    def column_names(self, table_name: str, show_names: bool = False) -> list:
        """
        Returns a list of column names for a given table.

        :param table_name: The name of the table in which the column names should be queried
        :param show_names: Boolean indicating if the column names should be printed to the console
        :return: list - The list of the column names
        """
        return self._info_query_list(
            info_type='columns',
            table_name=table_name,
            show_names=show_names
        )

    def table_names(self, show_names: bool = False) -> list:
        """
        Returns a list of the table names within the database.

        :param show_names: Boolean indicating if the table names should be printed to the console
        :return: list - The list of the table names
        """
        return self._info_query_list(
            info_type='tables',
            show_names=show_names
        )