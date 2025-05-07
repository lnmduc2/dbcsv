from pathlib import Path
import csv
import os
import json
from typing import List, Any

from app.core.storage_layer.datatypes import DBTypeObject

DB_DIR = str(Path(__file__).parent.parent.parent.parent.parent / "data")

class TableIterator:
    def __init__(self, schema: str, table: str, metadata: dict[str, str] = None, batch_size: int = 1000):
        self.schema = schema.lower()
        self.table_name = table.lower()
        self.batch_size = batch_size
        self._columns = list(metadata.keys()) if metadata else []
        self._column_types = list(metadata.values()) if metadata else []
        self._file = self._load_file(schema=self.schema, table=self.table_name)
        self._reader = csv.reader(self._file)
        self._check_header()
        self._is_done = False
    
    def __iter__(self) -> 'TableIterator':
        return self
    
    def __next__(self) -> List[Any]:
        try:
            row = next(self._reader)
            row = DBTypeObject.convert_type(row, self._column_types)
            
            if len(row) != len(self._columns):
                raise ValueError(f"Row length does not match column length in {self.schema}/{self.table_name}.")
            return row
        except Exception:
            self.close()
            raise StopIteration

    def _load_file(self, schema: str, table: str):
        schema, table = schema.lower(), table.lower()
        data_path = os.path.join(DB_DIR, schema, table + ".csv")
        try:
            return open(data_path, "r", encoding="utf-8")
        except FileNotFoundError:
            raise FileNotFoundError(f"Table {self.table_name} not found in schema {schema}.")
        except Exception as e:
            raise Exception(f"Error loading table {self.table_name}: {e}")
        
    def _check_header(self) -> None:
        header = next(self._reader)
        if len(header) != len(self._columns):
            raise ValueError(f"Header length ({len(header)}) does not match column length ({len(self._columns)}) in {self.schema}/{self.table_name}.")
        if any(col.lower() != header[i].lower() for i, col in enumerate(self._columns)):
            raise ValueError(f"Header names do not match column names in {self.schema}/{self.table_name}.")

    def __del__(self):
        self.close()

    def __repr__(self):
        result = f"Table: {self.table_name}\n"
        columns = [f"\n\t{col} ({typ})" for col, typ in zip(self._columns, self._column_types)]
        result += f"Columns: {''.join(columns)}\n"
        
        # Since we don't cache data anymore, we can't calculate column widths
        header = " | ".join(self._columns)
        result += header + "\n"
        result += "-" * len(header) + "\n"
        result += "[Data is not cached. Iterate through the table to see rows.]\n"

        return result
    
    def close(self) -> None:
        if hasattr(self, "_file") and self._file:
            self._file.close()
    
    def to_json(self, limit: int = None):
        if not limit:
            limit = self.batch_size

        tmp_file = self._load_file(schema=self.schema, table=self.table_name)
        tmp_reader = csv.reader(tmp_file)
        next(tmp_reader)  # Skip the header

        data = []
        for i, row in enumerate(tmp_reader):
            if i >= limit:
                break
            if len(row) != len(self._columns):
                raise ValueError(f"Row length does not match column length in {self.schema}/{self.table_name}.")
            data.append(row)

        tmp_file.close()

        result = {
            "table_name": self.table_name,
            "columns": self._columns,
            "column_types": self._column_types,
            "data": data
        }
        return json.dumps(result, indent=4)

    @property
    def columns(self):
        return self._columns

    @property
    def column_types(self):
        return self._column_types