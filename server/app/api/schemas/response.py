from pydantic import BaseModel
from typing import Any, List, Optional, Union

class BaseResponse(BaseModel):
    status: str = "success"
    message: Optional[str] = None

# For /execute endpoint
class ExecuteQueryResponse(BaseResponse):
    cursor_id: str
    position: int

# For fetch operations
class FetchResponse(BaseResponse):
    data: Union[List[Any], List[List[Any]], None]  # Could be None, a single row, or list of rows
    position: int

# For /close endpoint
class CloseCursorResponse(BaseResponse):
    pass