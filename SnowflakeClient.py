from typing import Any, Tuple, Union, List
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

class SnowflakeClient:
    """
    A class for connecting to and executing SQL statements in Snowflake.
    """
    def __init__(self, account:str, user:str, password:str, warehouse:str, database:str, schema:str, role:str, **kwargs):
        """
        Initializes a new SnowflakeClient instance with the specified credentials & parameters.

        Args:
            account (str): The name of the Snowflake account.
            user (str): The Snowflake user to authenticate as.
            password (str): The password for the Snowflake user.
            warehouse (str): The name of the Snowflake warehouse to use.
            database (str): The name of the Snowflake database to connect to.
            schema (str): The name of the Snowflake schema to use.
            role (str): The name of the Snowflake role to use.
            **kwargs: Arbitrary keyword arguments that will be used to initialize additional instance attributes.
        
        Attributes:
            account (str): The name of the Snowflake account.
            warehouse (str): The name of the Snowflake warehouse to use.
            database (str): The name of the Snowflake database to connect to.
            schema (str): The name of the Snowflake schema to use.
            role (str): The name of the Snowflake role to use.
        """
        ## set up logger        
        logging.basicConfig(format='%(levelname)s | %(funcName)s@%(filename)s | (%(lineno)03d) | %(message)s', level=logging.INFO)        
        self.logger = logging.getLogger(__name__)
        
        ## check arguments
        for arg_name in ["account", "user", "password", "warehouse", "database", "schema", "role"]:
            arg_value = locals()[arg_name]
            self._check_type(arg_name, arg_value, str)
        
        ## set instance attributes
        self.account = account
        self.__user = user
        self.__password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self._conn = None
        self._cur = None
        
        ## set additional instance attributes if provided via **kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        ## check if conn can be established
        self.logger.info("Testing Snowflake connection...")
        with self as (conn, cur):
            pass


    def _check_type(self, arg_name: str, arg_value: Any, expected_type: type) -> None:
        """
        Wrapper for isinstance() check.
        Raises a TypeError if checked argument is not of expected type.
        
        Returns:
            None
        """
        if not isinstance(arg_value, expected_type):
            error_msg = f"Argument '{arg_name}' is not valid! Expected type '{expected_type.__name__}' but received type '{type(arg_value).__name__}'."
            self.logger.error(error_msg)
            raise TypeError(error_msg)
    
    
    def __repr__(self) -> str:
        """
        Returns a string representation of the object.
        If object is instantiated with session parameters then all session parameters will be returned, not only the provided parameters.

        Returns:
            str: A string representation of the object.
        """
        repr_dict = {
            "account": f"'{self.account}'",
            "user": "'***'",
            "password": "'***'",
            "warehouse": f"'{self.warehouse}'",
            "database": f"'{self.database}'",
            "schema": f"'{self.schema}'",
            "role": f"'{self.role}'"
        }
        
        for key, value in vars(self).items():
            if key not in repr_dict and not key.startswith("_") and key != "logger":
                repr_dict[key] = repr(value)
        
        kwargs_str = ", ".join(f"{key} = {value}" for key, value in repr_dict.items())
        return f"{type(self).__name__}({kwargs_str})"

    
    def __enter__(self) -> Tuple[snowflake.connector.connection.SnowflakeConnection, snowflake.connector.cursor.SnowflakeCursor]:
        """
        Establishes a connection to Snowflake and creates a cursor.

        Returns:
            Tuple[snowflake.connector.connection.SnowflakeConnection, snowflake.connector.cursor.SnowflakeCursor]: A tuple containing the connection and cursor objects.
        """
        try:
            conn_params = {
                "account": self.account,
                "user": self.__user,
                "password": self.__password,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "role": self.role
            }
            
            ## additional args for if provided via **kwargs
            for key, value in self.__dict__.items():
                if key not in ["account", "__user", "__password", "warehouse", "database", "schema", "role", "_conn", "_cur"]:
                    conn_params[key] = value
            
            self._conn = snowflake.connector.connect(**conn_params)
            self.logger.info("Snowflake connection opened.")
            self._cur = self._conn.cursor()
            self.logger.info("Snowflake cursor opened.")
            return self._conn, self._cur
        except Exception as e:
            self.logger.error(f"Error connecting to Snowflake:\n{e}")
            raise

            
    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None) -> None:
        """
        Closes the connection and cursor.

        Args:
            exc_type: The exception type, if an exception occurred in the context.
            exc_val: The exception value, if an exception occurred in the context.
            exc_tb: The traceback, if an exception occurred in the context.
        
        Returns:
            None
        """
        if self._cur is not None:
            self._cur.close()
            self._cur = None
            self.logger.info("Snowflake cursor closed.")
        
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self.logger.info("Snowflake connection closed.")
    
    
    def close(self) -> None:
        """
        Manually close connection and cursor.
        
        Args:
            None
        
        Returns:
            None
        """
        self.__exit__()
        
        
    def execute(self, sql:str, async_mode:bool=False, keep_session_alive:bool=True) -> Union[Tuple[str], List]:
        """
        Executes a single SQL statement in Snowflake.

        Args:
            sql (str): The SQL statement to execute.
            async_mode (bool): Whether to execute the query asynchronously. Defaults to False.
            keep_session_alive (bool): Whether to keep Snowflake session alive. Defaults to True.
        
        Returns:
            If the query is executed asynchronously, returns the QueryID in a tuple.
            If the query is executed synchronously, returns the result of the query as a tuple.
        """
        
        self._check_type("sql", sql, str)
        self._check_type("async_mode", async_mode, bool)
        self._check_type("async_mode", keep_session_alive, bool)
        
        self.logger.info(f"Query will be executed {'asynchronously' if async_mode else 'synchronously'}.")
        self.logger.info(f"Preview of query text below:\n{sql[:100]}...")
        
        if keep_session_alive:
            if not self._cur:
                self.__enter__()
            if async_mode:
                self._cur.execute_async(sql)
                return (self._cur.sfqid,)
            else:
                result = self._cur.execute(sql).fetchall()
                return result
        else:
            with self as (conn, cur):
                if async_mode:
                    cur.execute_async(sql)
                    return (cur.sfqid,)
                else:
                    result = cur.execute(sql).fetchall()
                    return result

                
    def execute_many(self, sql_list:List[str], async_mode:bool=False) -> Union[List[str], List[Tuple]]:
        """
        Executes multiple SQL statements in Snowflake.

        Args:
            sql_list (List[str]): A list of SQL statements to execute.
            async_mode (bool): Whether to execute the queries asynchronously. Defaults to False.
        
        Returns:
            If the queries are executed asynchronously, returns a list of QueryIDs.
            If the queries are executed synchronously, returns a list of tuples containing the results of the queries.
        """
        self._check_type("sql_list", sql_list, list)
        self._check_type("async_mode", async_mode, bool)
        
        with self as (conn, cur):
            self.logger.info(f"Will execute {len(sql_list)} {'queries' if len(sql_list) > 1 else 'query'} {'asynchronously' if async_mode else 'synchronously'}.")
            results = []
            for sql in sql_list:
                self.logger.info(f"Preview of query text below:\n{sql[:100]}")
                if async_mode:
                    cur.execute_async(sql)
                    results.append(cur.sfqid)
                else:
                    result = cur.execute(sql).fetchall()
                    results.append(result)
            return results
    
    
    def df_to_snowflake(self, df:pd.DataFrame, database:str=None, schema:str=None, table:str=None) -> str:
        """
        Writes a pandas dataframe to an existing Snowflake table.
        
        Args:
            df (pandas.DataFrame): The pandas dataframe to write to Snowflake.
            database (str): The name of the Snowflake database to use.
            schema_name (str): The name of the Snowflake schema to use.
            table_name (str): The name of the Snowflake table to write to.
        
        Returns:
            str: A str containing a success message and the number of rows that were inserted into the table.
        """
        self._check_type("df", df, pd.DataFrame)
        
        if database is not None:
            self._check_type("database", database, str)
            
        if schema is not None:
            self._check_type("schema", schema, str)
        
        self._check_type("table", table, str)
        
        ## default database & schema to instance attributes
        database = database or self.database
        schema = schema or self.schema
        
        ## db, schema & table need to be in uppercase
        ## if not Snowflake will not find the table when checking for its full qualified name
        
        database, schema, table = [i.upper() for i in [database, schema, table]]
        
        with self as (conn, cur):
            try:
                success, _, nrows, _= write_pandas(conn=conn, df=df, database=database, schema=schema, table_name=table)
                msg = f"Success: Inserted {nrows} rows."
                self.logger.info(msg)
                return msg
            except snowflake.connector.errors.Error as e:
                error_msg = str(e)
                error_code = e.errno
                self.logger.error(f"Error ({error_code}): {error_msg}")
                raise
            except Exception as e:
                self.logger.error(f"Error:\n{e}")
                raise