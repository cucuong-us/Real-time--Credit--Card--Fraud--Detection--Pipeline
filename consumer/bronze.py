import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

def run_bronze_ingestion():
    # 1. Khởi tạo Spark Session (Bắt buộc khi chạy file .py)
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18") \
        .getOrCreate()

    sc = spark.sparkContext

    # 2. Cấu hình (Nên dùng biến môi trường để bảo mật thay vì dán trực tiếp)
    current_dir = Path(__file__).parent 

# Tìm file .env ở thư mục cha (thư mục gốc dự án)
    env_path = current_dir.parent / '.env'

    load_dotenv(dotenv_path=env_path)

# Lấy biến từ .env (tên biến thống nhất như ta đã sửa ở các bước trước)
    CONNECTION_STR = os.getenv('EVENT_HUBS_CONNECTION_STRING')
    BRONZE_OUTPUT_PATH = os.getenv('BRONZE_OUTPUT_PATH')
    CHECKPOINT_PATH = os.getenv('CHECKPOINT_PATH')

    # 3. Cấu hình Event Hubs
    # Lưu ý: Lệnh sc._jvm chỉ hoạt động khi có kết nối Databricks Connect
    eh_connection_string = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        CONNECTION_STR + ";EntityPath=transactions"
    )
    
    ehConf = {
      'eventhubs.connectionString' : eh_connection_string
    }

    # 4. Pipeline xử lý
    print("🚀 Đang bắt đầu luồng đọc từ Event Hubs...")
    raw_df = spark.readStream \
      .format("eventhubs") \
      .options(**ehConf) \
      .load()

    decoded_df = raw_df.select(col("body").cast("string").alias("transaction_data"))

    # 5. Ghi dữ liệu
    query = decoded_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start(BRONZE_OUTPUT_PATH)

    query.awaitTermination() # Giữ cho script chạy liên tục

if __name__ == "__main__":
    run_bronze_ingestion()