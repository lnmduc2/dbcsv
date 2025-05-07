import sys
from pathlib import Path
import os
from typing import Any, Iterator, List

from fastapi import HTTPException
from lark import Lark

from app.core.parser.parser import SQLTransformer, grammar
from app.core.storage_layer.metadata import Metadata
from app.core.storage_layer.query_executor import QueryExecutor


class DatabaseEngine():
    def __init__(self):
        self.__schemas : list[str] = self.__loadSchemas()
        self.__metadatas : dict[str, Metadata]= self.__loadMetadatas()  # Initialize the dict
        self.__parser = Lark(grammar, parser='lalr', transformer=SQLTransformer(), start='start')
        self.__executor = QueryExecutor
    
    @property
    def schemas(self) -> list[str]:
        return self.__schemas
    
    def __loadMetadatas(self) -> dict[str, Metadata]:
        self.__metadatas = {schema: Metadata(schema) for schema in self.__schemas}
        return self.__metadatas
    
    def __loadSchemas(self) -> list[str]:
        path = Path(__file__).parent.parent.parent / 'data' 
        schemas = [schema_name for schema_name in os.listdir(path) 
                  if os.path.isdir(Path(path) / schema_name)]
        return schemas

    def execute(self, sql_statement: str, schema: str) -> Iterator[List[Any]]:
        # Handle all exceptions related to query in the __executor.execute_sql function (e.g: sql syntax error, table not found, col not found, ...) 
        # because this function will raise exceptions for endpoints to throw http errors
        try:
            results = self.__executor.execute_sql(sql_statement, self.__metadatas[schema], self.__parser)
        except Exception as e:
            raise e
        return results

db_engine = DatabaseEngine()

def get_engine() -> DatabaseEngine:
    return db_engine