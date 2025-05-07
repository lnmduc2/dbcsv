from typing import List, Any, Iterator, Callable, Dict

from lark import Lark

from app.core.storage_layer.metadata import Metadata
from app.core.storage_layer.utils import sql_to_logical_plan

class QueryExecutor:
    """Executes SQL queries"""
    
    @staticmethod
    def execute_sql(sql: str, metadata: Metadata,  parser: Lark) -> Iterator[List[Any]]:
        """Parse, optimize, and execute a SQL query"""
        try:
            # Parse SQL to logical plan
            parsed_tree = parser.parse(sql)
        except Exception as e:
            raise Exception(f"Failed to parse query '{sql}': {str(e)}")
        if parsed_tree is None or len(parsed_tree.children) == 0:
            raise ValueError("Parsed query is None")
        else:
            parsed_query = parsed_tree.children[0]
        
        logical_plan = sql_to_logical_plan(parsed_query, metadata)

        # Execute the plan
        result = logical_plan.execute()
        return result
        
