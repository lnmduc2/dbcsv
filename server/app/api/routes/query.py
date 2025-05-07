from typing import Annotated, Dict
from fastapi import FastAPI, APIRouter , Depends, HTTPException
from uuid import uuid4

from app.api.schemas.request import SQLRequest
from app.core.database_engine import get_engine
from app.api.schemas.response import (
    BaseResponse,
    ExecuteQueryResponse,
    FetchResponse,
    CloseCursorResponse
)

router = APIRouter(
    prefix='/query',
    tags=['Query']
)

@router.get('/ping') # app thì ko được (bị trả về 404 not found) nhưng router thì đc???
def ping(
    schema: str,
    database_engine = Depends(get_engine)
) -> BaseResponse:
    """
    Check if the schema is available.
    """
    if schema not in database_engine.schemas:
        raise HTTPException(status_code=404, detail=f"Schema {schema} not found, please check the schema name in your dns string. Available schemas: {database_engine.schemas}")
    return BaseResponse(message=f"Schema {schema} is available.")


# Buffer to store active cursors
# This is a simple in-memory storage for demonstration purposes.
QUERY_CURSORS: Dict[str, dict] = {}


@router.post('/execute')
def execute_query(
    sql_request: SQLRequest,
    database_engine = Depends(get_engine)
) -> ExecuteQueryResponse:
    """
    Create a cursor (ProjectIterator) and returns a cursor ID.
    """
    cursor_id = str(uuid4())
    
    try:
        iterator = database_engine.execute(sql_request.sql_statement, sql_request.schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    QUERY_CURSORS[cursor_id] = {
        'iterator': iterator,
        'schema': sql_request.schema,
        'position': 0
    }
    
    return ExecuteQueryResponse(cursor_id=cursor_id, position=0)


@router.get('/fetchone/{cursor_id}')
def fetch_one(cursor_id: str) -> FetchResponse:
    """
    Fetches the current row that the cursor is pointing to and move it to the next.
    """
    cursor = QUERY_CURSORS.get(cursor_id)
    if not cursor:
        raise HTTPException(status_code=404, detail=f"Cursor id={cursor_id} not found")
    
    try:
        row = next(cursor['iterator'])
        cursor['position'] += 1
        return FetchResponse(data=row, position=cursor['position'])
    except StopIteration:  # Changed from StopAsyncIteration to StopIteration
        return FetchResponse(data=None, position=cursor['position'])


@router.get('/fetchmany/{cursor_id}')
def fetch_many(cursor_id: str, size: int = 100) -> FetchResponse:
    """
    Fetches the next `size` rows from the cursor and moves it forward.
    If `size` is not provided, it defaults to 100.
    """
    cursor = QUERY_CURSORS.get(cursor_id)
    if not cursor:
        raise HTTPException(status_code=404, detail=f"Cursor id={cursor_id} not found")
    
    rows = []
    for _ in range(size):
        try:
            row = next(cursor['iterator'])
            rows.append(row)
            cursor['position'] += 1
        except StopIteration:  # Changed from StopAsyncIteration to StopIteration
            break
    
    return FetchResponse(data=rows, position=cursor['position'])


@router.get('/fetchall/{cursor_id}')
def fetch_all(cursor_id: str) -> FetchResponse:
    """
    Fetches all remaining rows from the cursor and moves the cursor to the end.
    """
    cursor = QUERY_CURSORS.get(cursor_id)
    if not cursor:
        raise HTTPException(status_code=404, detail=f"Cursor id={cursor_id} not found")

    rows = []
    try:
        for row in cursor['iterator']:
            rows.append(row)
    except StopIteration:  # Changed from StopAsyncIteration to StopIteration
        pass
    cursor['position'] += len(rows)
    return FetchResponse(data=rows, position=cursor['position'])


@router.delete('/close/{cursor_id}')
def close_cursor(cursor_id: str) -> CloseCursorResponse:
    """
    Closes the cursor and removes it from the storage.
    """
    if cursor_id in QUERY_CURSORS:
        del QUERY_CURSORS[cursor_id]
        return CloseCursorResponse()
    raise HTTPException(status_code=404, detail=f"Cursor id={cursor_id} does not exists to be closed")