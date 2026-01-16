import time
import json
import random
import csv
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load biến môi trường
load_dotenv()

# Lấy biến từ .env (tên biến thống nhất như ta đã sửa ở các bước trước)
EVENT_HUBS_CONNECTION_STRING= os.getenv('EVENT_HUBS_CONNECTION_STRING')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
CSV_PATH = os.getenv('CSV_PATH', '/app/transactions_source.csv')

def run_producer():
    if not EVENT_HUBS_CONNECTION_STRING:
        print("❌ LỖI: Không tìm thấy EVENT_HUBS_CONNECTION_STRING")
        return

    # Tách lấy Bootstrap Server từ Connection String
    # Ví dụ: Endpoint=sb://abc.servicebus.windows.net/ -> abc.servicebus.windows.net:9093
    try:
        BOOTSTRAP_SERVER = EVENT_HUBS_CONNECTION_STRING.split(';')[0].replace('Endpoint=sb://', '').strip('/') + ':9093'
    except:
        print("❌ LỖI: Connection String không đúng định dạng Azure")
        return

    print(f"🔄 Đang kết nối tới Azure Event Hubs tại: {BOOTSTRAP_SERVER}...")

    producer = None
    while not producer:
        try:
            # Cấu hình đặc thù cho Azure Event Hubs
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVER],
                security_protocol='SASL_SSL',
                sasl_mechanism='PLAIN',
                sasl_plain_username='$ConnectionString', # BẮT BUỘC giữ nguyên chuỗi này
                sasl_plain_password=EVENT_HUBS_CONNECTION_STRING,      # Toàn bộ chuỗi Connection String
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                request_timeout_ms=60000, # Tăng timeout lên 60s cho mạng ổn định
                retries=5
            )
            print("✅ KẾT NỐI THÀNH CÔNG!")
        except Exception as e:
            print(f"❌ Thất bại: {e}. Thử lại sau 10 giây...")
            time.sleep(10)

    # Đọc CSV và gửi dữ liệu
    try:
        with open(CSV_PATH, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                producer.send(KAFKA_TOPIC, value=row)
                if i % 10 == 0: # Cứ 10 dòng thì flush một lần cho mượt
                    producer.flush()
                print(f"[{i}] ☁️ Đã gửi giao dịch của User {row.get('User')} lên Azure")
                time.sleep(random.uniform(1, 5)) # Giả lập thời gian thực
    except Exception as e:
        print(f"❌ Lỗi khi đang gửi: {e}")
    finally:
        if producer:
            producer.close()

if __name__ == '__main__':
    # Vòng lặp chính: Nếu sập thì tự khởi động lại sau 10s
    while True:
        run_producer()
        print("🛑 Producer tạm nghỉ. Khởi động lại sau 10 giây...")
        time.sleep(10)