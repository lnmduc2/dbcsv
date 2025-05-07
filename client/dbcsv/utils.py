import re
from urllib.parse import urlparse, urlunparse
import requests
from requests.exceptions import ConnectionError, Timeout
from typing import Union, Tuple
import jwt
import time
import os

from dbcsv.schemas.auth import Token, User
from dbcsv.schemas.response import (
    ExecuteQueryResponse,
    FetchResponse,
    CloseCursorResponse,
)
from dbcsv.exception import (
    InterfaceError,
    OperationalError,
    ProgrammingError,
    AuthenticationError,
    NetworkError
)

ACCESS_TOKEN_EXPIRE_MINUTES=30
ACCESS_TOKEN_DELTA_SECONDS = 60

# Regex to check if schema is snake case
_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_dsn_url(dsn: str) -> Tuple[str, str]:
    """
    Validate if dsn complies to format https://localhost:PORT/<schema_snake_case>.
    Returns a tuple of (schema_snake_case, base_url)
    Throw InterfaceError if validation fails
    """
    
    parsed = urlparse(dsn)
    
    if parsed.scheme not in ["http", "https"]:
        raise InterfaceError("DSN must be http or https")

    if parsed.port is not None and not (1 <= parsed.port <= 65535):
        raise InterfaceError("Port number is not valid (not in range 1-65535)")

    if (parsed.params or parsed.query or parsed.fragment 
            or parsed.username or parsed.password):
        raise InterfaceError("DSN must not contain query, fragment, user/password")

    path_parts = parsed.path.strip("/").split("/")
    if len(path_parts) != 1 or not path_parts[0]:
        raise InterfaceError("DSN must end with exactly one path component (schema_snake_case)")

    schema_snake_case = path_parts[0]
    
    # Reconstruct base URL (without the schema path)
    base_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        "",  # path
        "",  # params
        "",  # query
        ""   # fragment
    ))
    
    return schema_snake_case, base_url.rstrip("/")
        

def login(url: str, schema: str, username: str, password: str) -> Token:
    try:
        # Ping database engine to check if schema is available
        r = requests.get(f"{url}/query/ping", params={'schema': schema})
        
        if r.status_code == 404:
            error_message = r.json().get("detail", "Internal Server Error")
            raise ProgrammingError(error_message)
        
        # Authenticate user
        data = {
            "username": username,
            "password": password,
        }
    
        r = requests.post(f"{url}/auth/connect", data=data)
        
        if r.status_code == 401:
            raise AuthenticationError("Invalid username or password")
            
        return Token(**r.json())
        
    except (ConnectionError, Timeout) as e:
        raise NetworkError(str(e)) from None


def validate_token(url: str, token: str) -> Union[Token, None]:
    # Load ACCESS_TOKEN_DELTA_SECONDS from .env
    refresh_threshold = int(os.getenv("ACCESS_TOKEN_DELTA_SECONDS") or ACCESS_TOKEN_DELTA_SECONDS)

    # Decode token without verification to check expiration
    payload = jwt.decode(token, options={"verify_signature": False})
    current_time = time.time()
    expiration_time = payload['exp']

    # Calculate remaining time
    remaining_time = expiration_time - current_time

    # If token is still valid AND not near expiration -> return None
    if remaining_time > refresh_threshold:
        print("Token still valid and not near expiration. Proceeds to query...")
        return None

    # If the token has expired -> raise an AuthenticationError
    if remaining_time <= 0:
        raise AuthenticationError("Token has expired and cannot be used to execute any query. Please create a new one by creating a new Connection to the database engine")
    
    # Token expired -> call to endpoint /refresh
    print(f"Token will expire in {remaining_time:.0f} seconds. Refreshing a new one...")
    header = {"Authorization": f"Bearer {token}"}
    r = requests.post(f"{url}/auth/refresh", headers=header)
    response_status = r.status_code

    if response_status == 403:
        raise AuthenticationError("Token verification failed")
    return Token(**r.json())


# Execute SQL query, but does not fetch any result yet
def execute_query(url: str, schema: str, query: str) -> ExecuteQueryResponse:
    data = {"sql_statement": query, "schema": schema}
    r = requests.post(f"{url}/query/execute", json=data)
    response_status = r.status_code
    
    # 500 -> syntax error in SQL query
    if response_status == 500:
        error_message = r.json().get("detail", "Internal Server Error")
        raise ProgrammingError(error_message)

    return ExecuteQueryResponse(**r.json())


# Fetch one row from the query results
def fetch_one(url: str, cursor_id: str) -> FetchResponse:
    r = requests.get(f"{url}/query/fetchone/{cursor_id}")
    response_status = r.status_code

    if response_status == 404:
        error_message = r.json().get("detail", "Internal Server Error")
        raise OperationalError(error_message)
    
    return FetchResponse(**r.json())


# Fetch many rows from the query results
def fetch_many(url: str, cursor_id: str, size: int = 1) -> FetchResponse:    
    r = requests.get(f"{url}/query/fetchmany/{cursor_id}?size={size}")
    response_status = r.status_code
    
    if response_status == 404:
        error_message = r.json().get("detail", "Internal Server Error")
        raise OperationalError(error_message)

    return FetchResponse(**r.json())
    


# Fetch all rows from the query results
def fetch_all(url: str, cursor_id: str) -> FetchResponse:
    r = requests.get(f"{url}/query/fetchall/{cursor_id}")
    response_status = r.status_code
    
    if response_status == 404:
        error_message = r.json().get("detail", "Internal Server Error")
        raise OperationalError(error_message)

    return FetchResponse(**r.json())


# Close cursor (only if cursor_id is not None)
def close(url: str, cursor_id: str) -> CloseCursorResponse:
    r = requests.delete(f"{url}/query/close/{cursor_id}")
    response_status = r.status_code
    
    if response_status == 404:
        error_message = r.json().get("detail", "Internal Server Error")
        raise OperationalError(error_message)

    return CloseCursorResponse(**r.json())