import pytest
from dbcsv.connection import Connection
from dbcsv.exception import *
from dbcsv import connect

# Arrange
valid_dsn = "http://127.0.0.1:8001/schema1"
valid_user = "johndoe"
valid_password = "secret"

def test_connect_success():
    """
    Test kết nối thành công với thông tin đăng nhập hợp lệ
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    
    assert conn is not None, "Kết nối không được tạo"
    assert isinstance(conn, Connection), "Đối tượng trả về không phải là Connection"
    assert conn.is_online is True, "Trạng thái kết nối phải là online"
    assert conn.url == "http://127.0.0.1:8001", "URL không khớp"
    assert conn.schema == "schema1", "Schema không khớp"
 
    # Kiểm tra có thể tạo cursor từ kết nối
    cursor = conn.cursor()
    assert cursor is not None, "Không thể tạo cursor từ kết nối"
    assert cursor.cursor_id is None, "Cursor chưa gọi execute() những vẫn có id"

    # Query khi None
    with pytest.raises(InterfaceError):
        cursor.fetchone()

    with pytest.raises(InterfaceError):
        cursor.fetchmany(5)

    with pytest.raises(InterfaceError):
        cursor.fetchall()
    
    # Dọn dẹp
    with pytest.raises(InternalError):
        cursor.close()

        
    conn.close()
    
def test_connect_with_wrong_credentials():
    """
    Test kết nối với thông tin đăng nhập không hợp lệ
    """
    with pytest.raises(AuthenticationError):
        connect(
            dsn="http://127.0.0.1:8001/schema1",
            user="wronguser",
            password="wrongpass"
        )

def test_connect_invalid_dsn():
    """
    Test kết nối với DSN sai định dạng (không chứa schema)
    """
    with pytest.raises(InterfaceError):
        connect(
            dsn="http://127.0.0.1:8001",  # Thiếu schema
            user="johndoe",
            password="secret"
        )

def test_connect_server_unreachable():
    """
    Test kết nối đến server không tồn tại hoặc không phản hồi
    """
    with pytest.raises(NetworkError):
        connect(
            dsn="http://999.999.999.999:8001/schema1",  # IP không tồn tại
            user="johndoe",
            password="secret"
        )

def test_nonexist_schema():
    """
    Kiểm tra lỗi khi schema không tồn tại
    """
    with pytest.raises(ProgrammingError):
        connect(
           dsn="http://127.0.0.1:8001/schema9", 
           user=valid_user, 
           password=valid_password
        )
    
def test_execute_invalid_query():
    """
    Test execute() với câu query sai cú pháp hoặc không được hỗ trợ
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    cursor = conn.cursor()
    
    with pytest.raises(ProgrammingError):
        cursor.execute("INVALID SQL QUERY")  # Query sai cú pháp
    
    conn.close()

def test_execute_nonexist_table():
    """
    Test execute() với bảng không tồn tại
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    cursor = conn.cursor()
      # Giả sử bảng không tồn tại
    
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT * FROM non_existent_table")

    conn.close()

def test_select_nonexistent_column():
    """
    Test truy vấn cột không tồn tại trong bảng
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    cursor = conn.cursor()
    
    # Giả sử có bảng "employees" với các cột "id", "name" nhưng không có cột "non_exist_col"
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELECT non_exist_col FROM table1")
    
    assert "non_exist_col" in str(excinfo.value), "Thông báo lỗi phải chỉ rõ tên cột không tồn tại"
    conn.close()

def test_fetchmany_invalid_size():
    """
    Test fetchmany() với size <= 0 hoặc không phải int
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table1")
    
    with pytest.raises(InterfaceError):
        cursor.fetchmany(size=0)  # Size phải >= 1
    
    with pytest.raises(InterfaceError):
        cursor.fetchmany(size="invalid")  # Size phải là int
    
    conn.close()

def test_double_close_connection():
    """
    Test gọi close() nhiều lần trên cùng một connection
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    conn.close()
    
    with pytest.raises(InternalError):
        conn.close()  # Mong đợi lỗi khi đóng kết nối đã đóng

def test_use_cursor_after_connection_close():
    """
    Test sử dụng cursor sau khi connection đã đóng
    """
    conn = connect(dsn=valid_dsn, user=valid_user, password=valid_password)
    cursor = conn.cursor()
    conn.close()
    
    with pytest.raises(InternalError):
        cursor.execute("SELECT 1")  # Mong đợi lỗi khi connection đã đóng