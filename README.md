# DBCSV - Relational Database using CSV

This database is implemented for educational purposes. Users may alter the source code to include more features.

## Features

- **Simple RDBMS**: Manage data in the form of CSV tables in the file system
- **SQL Processing**: Support SQL SELECT queries
- **REST API**: Provide a REST API interface to interact with the database
- **Python DBAPI2 Compliance**: Client provides an interface compatible with PEP-249
- **Cursor-based Queries**: Support cursors to handle large result sets

## System Architecture

The system consists of two main components:

1. **Server**: FastAPI API manages access, processes SQL queries, and retrieves data
2. **Client**: Python library provides a DBAPI2 standard interface

## Usage

### Install and run the system using Docker

```bash
# Clone repository
git clone https://github.com/your-username/dbcsv.git
cd dbcsv

# Run the system using Docker Compose (default port is 8001)
docker-compose up
```

### Set up the development environment

```bash
# In /server folder
pip install -r requirements.txt

# In /client folder
pip install -e .
```

### Using the Client API

```python
import dbcsv

# Connect to the database
conn = dbcsv.connect(
    "http://127.0.0.1:8001/schema1",
    user="johndoe",
    password="secret"
)

# Create a cursor
cursor = conn.cursor()

# Execute an SQL query
cursor.execute('Select * from table1 where id > 4 or (id < 3 AND age > 30')

# Fetch one result row
row = cursor.fetchone()
print(row)

# Fetch multiple result rows
rows = cursor.fetchmany(size=5)
print(rows)

# Fetch all remaining result rows
all_rows = cursor.fetchall()
print(all_rows)

# Close the cursor and connection
cursor.close()
conn.close()
```

## Data Structure

Data is organized into schemas, each containing multiple tables in the form of CSV files. The metadata of each schema is defined in a YAML file.

Sample metadata file:

```yaml
tables:
  - table_name: users
    columns:
      - column_name: id
        column_type: int
      - column_name: name
        column_type: string
      - column_name: age
        column_type: int
```

## Supported SQL

The system currently supports the following SQL statements:

- `SELECT` with clauses:
  - Column filtering (`SELECT col1, col2, ...`)
  - `WHERE` conditions with comparison operators (`=`, `<>`, `>`, `<`, `>=`, `<=`)
  - `AND`, `OR` conditions

## API Endpoints

- `GET /query/ping`: Check if the schema exists
- `POST /query/execute`: Execute an SQL query
- `GET /query/fetchone/{cursor_id}`: Fetch one result row
- `GET /query/fetchmany/{cursor_id}`: Fetch multiple result rows
- `GET /query/fetchall/{cursor_id}`: Fetch all result rows
- `DELETE /query/close/{cursor_id}`: Close the cursor

## Development

### Requirements

- Python 3.10+
- FastAPI
- Lark parser

### Test the source code with Pytest

Refer to the file `server/dbcsv/tests/unit/dbapi2/test_dbapi2.py` and `server/dbcsv/tests/integration/test_fullbaseline.py`
