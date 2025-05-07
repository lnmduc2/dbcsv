import datetime

class DBTypeObject:
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        if other in self.values:
            return True
        return False
    
    @staticmethod
    def convert_datatype(data: str, dtype: str = "") -> any:
        if dtype.lower() == STRING:
            if data.startswith("'") and data.endswith("'"):
                return data[1:-1]
            return data
        elif data.startswith("'") and data.endswith("'") and dtype.lower() != STRING:
            raise ValueError(f"Invalid {dtype.lower} format: {data} is a string, not a {dtype.lower()}")
        elif dtype.lower() == INTEGER:
            try:
                return int(data)
            except ValueError:
                raise ValueError(f"Invalid integer format: {data} is not an integer")
        elif dtype.lower() == FLOAT:
            try:
                return float(data)
            except ValueError:
                raise ValueError(f"Invalid float format: {data} is not a float")
        elif dtype.lower() == BOOLEAN:
            if data.lower() in ["true", "false"]:
                return data.lower() == "true"
            else:
                raise ValueError(f"Invalid boolean format: {data} is not a boolean value")
        elif dtype.lower() == DATE or dtype.lower() == DATETIME:
            try:
                return datetime.datetime.strptime(data, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid date format (format %Y-%m-%d): {data}")
        elif dtype.lower() == NULL:
            if data.lower() == "null":
                return None
            else:
                raise ValueError(f"Invalid null format: {data}")
        else:
            try:
                if data.startswith("'") and data.endswith("'"):
                    return data[1:-1]
                else:
                    try:
                        return int(data)
                    except ValueError:
                        try:
                            return float(data)
                        except ValueError:
                            return data
            except Exception as e:
                raise ValueError(f"Invalid data format: {data}") from e

    '''
    Unexpected behavior: when changing data to a wrong format, it only returns the rows before the changed row and stop  
    Example, if the 3rd row of schema2/table1 is changed to a wrong format (2024--05-10), it will return the first 2 rows and stop:
    
    query = "SELECT * FROM table1"
    cursor.execute(query)

    chunk = cursor.fetchall()
    print(chunk)
    >>> [[1, 'John Doe', 28, 'john.doe@example.com', 85.5, True, '2023-01-15', '2023-04-20', 'Excellent performer'], [2, 'Jane Smith', 34, 'jane.smith@example.com', 90.0, False, '2022-11-03', '2023-05-01', 'null']]
    '''      
    @staticmethod
    def convert_type(row: list[str], column_types: list[str]) -> list[any]:
        if len(row) != len(column_types):
            raise ValueError(f"Row length {len(row)} does not match column types length {len(column_types)}")
        return [DBTypeObject.convert_datatype(data, dtype) for data, dtype in zip(row, column_types)]
            

STRING = DBTypeObject("varchar", "text", "char")
INTEGER = DBTypeObject("integer", "int", "bigint", "smallint", "tinyint")
FLOAT = DBTypeObject("float", "double", "decimal", "dec")
BOOLEAN = DBTypeObject("boolean", "bool")
DATE = DBTypeObject("date")
DATETIME = DBTypeObject("datetime", "timestamp")
NULL = DBTypeObject("null")


