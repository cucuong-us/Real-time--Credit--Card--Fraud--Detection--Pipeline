import time
import json
import random
import csv
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable  # THÊM DÒNG NÀY
# Cấu hình từ môi trường hoặc mặc định
BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'transactions')
CSV_FILE_PATH = os.getenv('CSV_PATH', '/opt/spark/apps/transactions_source.csv')

def run_producer():
    # Khởi tạo producer với thêm các tham số tin cậy
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all'
            )
            print("✅ Đã kết nối thành công tới Kafka!")
        except NoBrokersAvailable:
            print("❌ Kafka chưa sẵn sàng, đang thử lại sau 5 giây...")
            time.sleep(5)

    print(f"--- 🚀 Producer started. Sending to {TOPIC_NAME} ---")
    
    try:
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"Không thấy file tại {CSV_FILE_PATH}")

        with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            for count, row in enumerate(csv_reader, 1):
                # Gửi dữ liệu
                producer.send(TOPIC_NAME, value=row)
                producer.flush() 
                
                print(f"[{count}] Sent: User {row.get('User')} | Amount: {row.get('Amount')}")
                
                # Delay ngẫu nhiên
                time.sleep(random.uniform(1, 5))
                
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()
        print("--- 🛑 Producer closed ---")

if __name__ == '__main__':
    run_producer()