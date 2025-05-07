from typing import Optional, List, Any, Tuple, Union
from requests.exceptions import ConnectionError, Timeout

from dbcsv.utils import validate_dsn_url, login, validate_token, execute_query, fetch_one, fetch_many, fetch_all, close
from dbcsv.exception import InternalError, NotSupportedError, InterfaceError

# Flow: connection.execute -> utils.execute_query -> [/query/execute] -> execute_query endpoint -> database_engine.execute -> executor.execute_sql
class Connection:
    def __init__(self, token: str):
        self.token = token
        self._url = None
        self._is_online = True
        self._schema = None

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    @property
    def is_online(self):
        return self._is_online

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    def cursor(self) -> "Cursor":
        if not self._is_online:
            raise InternalError("Cannot create any cursor from a closed connection")
        return Cursor(self)

    def rollback(self):
        raise NotSupportedError("rollback() is currently not supported")

    def commit(self):
        raise NotSupportedError("commit() is currently not supported")

    def close(self):
        if not self._is_online:
            raise InternalError("Connection is already closed")
        self._is_online = False



class Cursor:
    def __init__(self, connection: Connection):
        self.arraysize = 1
        self._connection = connection
        self._description: Optional[Tuple] = None
        self._rowcount = -1
        self._lastrowid: Optional[int] = None
        self._cursor_id = None

    @property
    def description(self) -> Optional[Tuple]:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> Optional[int]:
        return self._lastrowid

    @property
    def cursor_id(self) -> str:
        return self._cursor_id

    def close(self) -> None:
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform close() on cursor of a closed connection"
            )
        if self._cursor_id is None:
            raise InternalError("Cursor is not open or already closed. Call execute() first")
        
        close(self._connection.url, self._cursor_id)
        self._cursor_id = None
        self._description = None
        self._rowcount = -1
        self._lastrowid = None


    def execute(self, q: str, parameters=None) -> None:
        if not self._connection.is_online:
            raise InternalError("Cannot perform execute() on cursor of a closed connection")
        # Tự động đóng cursor nếu đang mở
        if self._cursor_id is not None:
            self.close()

        new_token = validate_token(self._connection.url, self._connection.token)
        if new_token is not None:
            self._connection.token = new_token.access_token

        # Call to /execute endpoint and create an iterator on engine side
        cursor = execute_query(self._connection.url, self._connection.schema, q)

        if cursor.cursor_id is None:
            raise InternalError("Failed to create cursor on server side")

        # Reset rowcount for new execution
        self._rowcount = -1

        # Set cursor id and rowcount
        self._cursor_id = cursor.cursor_id
        self._rowcount = cursor.position


    def fetchone(self) -> Union[List[Any], None]:
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform fetchone() on cursor of a closed connection"
            )
        if not self._cursor_id:
            raise InterfaceError("Cursor is not open or has been closed. Call execute() first to create an ID for this cursor before fetching results.")
        
        result = fetch_one(self._connection.url, self._cursor_id)
        self._rowcount = result.position
        return result.data


    def fetchmany(self, size: Optional[int] = None) -> List[List[Any]]:
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform fetchmany() on cursor of a closed connection"
            )
        if not self._cursor_id:
            raise InterfaceError("Cursor is not open or has been closed. Call execute() first to create an ID for this cursor before fetching results.")
            
        if size is None:
            size = self.arraysize
        if not isinstance(size, int):
            raise InterfaceError("Size must be an integer")
        if size <= 0:
            raise InterfaceError("Size must be a positive integer")
        
        result = fetch_many(self._connection.url, self._cursor_id, size)
        self._rowcount = result.position
        return result.data


    def fetchall(self) -> List[List[Any]]:
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform fetchall() on cursor of a closed connection"
            )
        if not self._cursor_id:
            raise InterfaceError("Cursor is not open or has been closed. Call execute() first to create an ID for this cursor before fetching results.")
            
        result = fetch_all(self._connection.url, self._cursor_id)
        self._rowcount = result.position
        return result.data
    

    def setinputsizes(self, sizes: List[Any]) -> None:
        pass  # Do nothing per DBAPI2 specification


    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        pass  # Do nothing per DBAPI2 specification


def connect(
    dsn: str,
    user: str,
    password: str,
) -> Connection:
    """
    Initializes a connection to the database.

    Returns a Connection Object. It takes a number of parameters which are database dependent.

    E.g. a connect could look like this: connect(dsn='https://localhost:1234/schema', user='guido', password='1234')
    """
    # Check if schema exists in database

    
    # Validate url correctness
    schema, url = validate_dsn_url(dsn)

    # Request to /login endpoint of dsn to get JWT token. Catches exception if user doesn't exist in database
    token = login(url, schema, user, password)

    # Create connection
    conn = Connection(token=token.access_token)

    conn.schema = schema
    conn.url = url

    return conn
