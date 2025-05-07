import dbcsv
import time


def test_connection(i):
    print(f"=== Bắt đầu kiểm tra kết nối và cursor cho client {i} ===")
    
    try:
        conn = dbcsv.connect(
            "http://127.0.0.1:8001/schema1", 
            user="johndoe", 
            password="secret"
        )
    except Exception as e:
        print(f"✗ Lỗi kết nối: {str(e)}")
        return False

    cursor = conn.cursor()

    query = 'Select * from table1 where id > 4 or (id < 3 AND age > 30)'

    cursor.execute(query)

    chunk = cursor.fetchmany(100)
    print((chunk))
    print()

    cursor.close()

    conn.close()


if __name__ == "__main__":
    start_time = time.time()
    for i in range(1):
        test_connection(i)
    elapsed = time.time() - start_time
    
    print(f"Thời gian thực thi: {elapsed:.2f} giây")

