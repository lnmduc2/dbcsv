import pytest
import dbcsv
from dbcsv.exception import *

'''
-----------------------------------------------------------------------------------------------------------------------------------------
Easy and basic test cases (standard form queries)
-----------------------------------------------------------------------------------------------------------------------------------------
'''
def test_select_all_from_table1():
    """Test SELECT * FROM table1"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM table1")
    result = cursor.fetchall()

    expected = [
        [1, "John Doe", 28, "john.doe@example.com"],
        [2, "Jane Smith", 34, "jane.smith@example.com"],
        [3, "Michael Brown", 22, "michael.brown@example.com"],
        [4, "Emily Davis", 29, "emily.davis@example.com"],
        [5, "Chris Wilson", 31, "chris.wilson@example.com"]
    ]

    assert result == expected
    assert cursor.rowcount == 5
    cursor.close()
    conn.close()

def test_select_columns_from_table2():
    """Test SELECT id, name, salary FROM table2"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT id, name, salary FROM table2")
    result = cursor.fetchall()

    expected = [
        [1, "John Doe", 75000],
        [2, "Jane Smith", 68000],
        [3, "Michael Brown", 50000],
        [4, "Emily Davis", 62000],
        [5, "Chris Wilson", 71000]
    ]

    assert result == expected
    assert cursor.rowcount == 5
    cursor.close()
    conn.close()

def test_fetchone_behavior():
    """Test fetchone behavior"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT id, name FROM table1 WHERE id < 3")

    # Lấy dòng đầu tiên
    first_row = cursor.fetchone()
    assert first_row == [1, "John Doe"]
    assert cursor.rowcount == 1

    # Lấy dòng thứ hai
    second_row = cursor.fetchone()
    assert second_row == [2, "Jane Smith"]
    assert cursor.rowcount == 2

    # Không còn dòng nào
    assert cursor.fetchone() is None
    assert cursor.rowcount == 2

    cursor.close()
    conn.close()

def test_fetchmany_behavior():
    """Test fetchmany behavior"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM table1")

    # Lấy 2 dòng đầu
    first_chunk = cursor.fetchmany(2)
    assert first_chunk == [
        [1, "John Doe", 28, "john.doe@example.com"],
        [2, "Jane Smith", 34, "jane.smith@example.com"]
    ]
    assert cursor.rowcount == 2

    # Lấy 3 dòng tiếp (chỉ còn 3)
    second_chunk = cursor.fetchmany(3)
    assert second_chunk == [
        [3, "Michael Brown", 22, "michael.brown@example.com"],
        [4, "Emily Davis", 29, "emily.davis@example.com"],
        [5, "Chris Wilson", 31, "chris.wilson@example.com"]
    ]
    assert cursor.rowcount == 5

    # Không còn dòng nào
    assert cursor.fetchmany(1) == []
    assert cursor.rowcount == 5

    cursor.close()
    conn.close()

def test_where_condition_age_gt_30():
    """Test SELECT * FROM table1 WHERE age > 30"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM table1 WHERE age > 30")
    result = cursor.fetchall()

    expected = [
        [2, "Jane Smith", 34, "jane.smith@example.com"],
        [5, "Chris Wilson", 31, "chris.wilson@example.com"]
    ]

    assert result == expected
    assert cursor.rowcount == 2
    cursor.close()
    conn.close()

def test_where_condition_and():
    """Test SELECT * FROM table2 WHERE department = 'Engineering' AND salary > 70000"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM table2 WHERE department = 'Engineering' AND salary > 72000")
    result = cursor.fetchall()

    expected = [
        [1, "John Doe", 28, "Engineering", 75000],
    ]

    assert result == expected
    assert cursor.rowcount == 1
    cursor.close()
    conn.close()

def test_where_condition_or():
    """Test SELECT name, age FROM table1 WHERE age < 25 OR age > 30"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT name, age FROM table1 WHERE age < 25 OR age > 30")
    result = cursor.fetchall()

    expected = [
        ["Jane Smith", 34],
        ["Michael Brown", 22],
        ["Chris Wilson", 31]
    ]

    assert result == expected
    assert cursor.rowcount == 3
    cursor.close()
    conn.close()

def test_transaction_unsupported():
    """Test transaction methods raise NotSupportedError"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )

    with pytest.raises(NotSupportedError):
        conn.commit()

    with pytest.raises(NotSupportedError):
        conn.rollback()

    conn.close()


'''
-----------------------------------------------------------------------------------------------------------------------------------------
Advanced test cases (complex queries, redundant spaces, case insensitivity, etc.)
-----------------------------------------------------------------------------------------------------------------------------------------
'''
def test_case_sensitive_column_names():
    """Test case sensitivity trong tên cột (nên fail nếu hệ thống case sensitive)"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT ID, NAME FROM table1")

    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT Id, NaMe FROM table1")  # mixed case

    conn.close()


def test_whitespace_handling():
    """Test xử lý khoảng trắng thừa trong query"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # Query với khoảng trắng thừa ở nhiều vị trí
    query = """
    SELECT  id  ,  name  ,  age
    FROM  table1
    WHERE  age  >  20  AND   name  =  'John Doe'  OR  name  =  'Jane Smith'
    """

    cursor.execute(query)
    result = cursor.fetchall()

    expected = [
        [1, "John Doe", 28],
        [2, "Jane Smith", 34]
    ]

    assert result == expected
    cursor.close()
    conn.close()

def test_complex_parentheses_conditions():
    """Test các điều kiện phức tạp với nhiều dấu ngoặc"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # Query phức tạp với nhiều tầng điều kiện
    query = """
    SELECT * FROM table2
    WHERE (
        (department = 'Engineering' AND salary > 70000)
        OR
        (department = 'HR' AND age < 30)
        OR
        (department = 'Sales' AND salary = 50000)
    )
    """

    cursor.execute(query)
    result = cursor.fetchall()

    expected = [
        [1, "John Doe", 28, "Engineering", 75000],
        [5, "Chris Wilson", 31, "Engineering", 71000],
        [4, "Emily Davis", 29, "HR", 62000],
        [3, "Michael Brown", 22, "Sales", 50000],
    ]

    assert sorted(result) == sorted(expected)
    cursor.close()
    conn.close()

def test_operator_precedence():
    """Test độ ưu tiên của toán tử AND/OR"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # Query kiểm tra độ ưu tiên toán tử
    query1 = "SELECT id FROM table1 WHERE age > 25 OR age < 30 AND name = 'John Doe'"
    query2 = "SELECT id FROM table1 WHERE (age > 25 OR age < 30) AND name = 'John Doe'"

    # Thực thi query1 (AND ưu tiên hơn OR)
    cursor.execute(query1)
    result1 = cursor.fetchall()
    expected1 = [
        [1],  # John Doe (thỏa age < 30 AND name)
        [2],  # Jane Smith (age > 25)
        [4],  # Emily Davis (age > 25)
        [5]   # Chris Wilson (age > 25)
    ]
    assert result1 == expected1

    # Thực thi query2 (dùng ngoặc rõ ràng)
    cursor.execute(query2)
    result2 = cursor.fetchall()
    expected2 = [
        [1]  # Chỉ John Doe thỏa cả điều kiện
    ]
    assert result2 == expected2

    cursor.close()
    conn.close()

def test_empty_result_conditions():
    """Test các điều kiện trả về kết quả rỗng"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # 1. Điều kiện luôn sai
    cursor.execute("SELECT * FROM table1 WHERE 1 = 0")
    assert cursor.fetchall() == []
    assert cursor.rowcount == 0

    # 2. Kết hợp nhiều AND dẫn đến không có kết quả
    cursor.execute("""
        SELECT * FROM table2
        WHERE department = 'Engineering'
        AND department = 'HR'
        AND salary > 100000
    """)
    assert cursor.fetchall() == []
    assert cursor.rowcount == 0

    # 3. OR với các điều kiện không tồn tại
    cursor.execute("""
        SELECT * FROM table1
        WHERE name = 'Non-existent Name'
        OR email = 'not@exists.com'
    """)
    assert cursor.fetchall() == []
    assert cursor.rowcount == 0

    cursor.close()
    conn.close()

def test_comma_handling_in_select():
    """Test cách xử lý dấu phẩy trong mệnh đề SELECT"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # 1. Thừa dấu phẩy ở cuối
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT id, name, FROM table1")

    # 2. Thiếu dấu phẩy giữa các cột
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT id name FROM table1")

    # 3. Dấu phẩy liên tiếp
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT id,,name FROM table1")

    conn.close()

def test_string_literal_handling():
    """Test cách xử lý string literal trong điều kiện"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # 1. String với dấu nháy đơn bên trong
    cursor.execute("SELECT * FROM table1 WHERE name = 'John Doe'")
    assert len(cursor.fetchall()) == 1

    # 2. String với khoảng trắng thừa
    cursor.execute("SELECT * FROM table1 WHERE name = '  John Doe  '")
    assert len(cursor.fetchall()) == 0  # Không match vì khác chuỗi

    # 3. String với ký tự đặc biệt
    cursor.execute("SELECT * FROM table1 WHERE email = 'john.doe@example.com'")
    assert len(cursor.fetchall()) == 1

    cursor.close()
    conn.close()

def test_string_comparison_case_sensitivity():
    """Test toán tử so sánh chuỗi (=) có phân biệt hoa thường không"""
    conn = dbcsv.connect(
        "http://127.0.0.1:8001/schema1",
        user="johndoe",
        password="secret"
    )
    cursor = conn.cursor()

    # Lấy ra tên có chữ hoa/thường từ dữ liệu có sẵn (John Doe)
    original_name = "John Doe"
    lowercase_name = original_name.lower()  # "john doe"
    uppercase_name = original_name.upper()  # "JOHN DOE"
    mixed_case_name = "jOhN dOe"

    # Test 1: So sánh với tên đúng case gốc (chắc chắn phải match)
    cursor.execute(f"SELECT id FROM table1 WHERE name = '{original_name}'")
    exact_case_result = cursor.fetchall()
    assert len(exact_case_result) == 1, "Phải tìm thấy bản ghi khi so sánh đúng case"

    # Test 2: So sánh với lowercase
    cursor.execute(f"SELECT id FROM table1 WHERE name = '{lowercase_name}'")
    lowercase_result = cursor.fetchall()

    # Test 3: So sánh với UPPERCASE
    cursor.execute(f"SELECT id FROM table1 WHERE name = '{uppercase_name}'")
    uppercase_result = cursor.fetchall()

    # Test 4: So sánh với mixed case
    cursor.execute(f"SELECT id FROM table1 WHERE name = '{mixed_case_name}'")
    mixed_case_result = cursor.fetchall()

    # Xác định behavior của hệ thống
    if len(lowercase_result) == 1 and len(uppercase_result) == 1:
        print("✓ Toán tử = KHÔNG phân biệt hoa thường (case insensitive)")
    elif len(lowercase_result) == 0 and len(uppercase_result) == 0:
        print("✓ Toán tử = PHÂN BIỆT hoa thường (case sensitive)")
    else:
        print("⚠ Hệ thống có hành vi không nhất quán về case sensitivity")

    # Assert để tích hợp với pytest
    # (Giả sử hệ thống là case sensitive - điều chỉnh nếu ngược lại)
    assert len(lowercase_result) == 0, "Lowercase không được match nếu case sensitive"
    assert len(uppercase_result) == 0, "Uppercase không được match nếu case sensitive"
    assert len(mixed_case_result) == 0, "Mixed case không được match nếu case sensitive"

    cursor.close()
    conn.close()
