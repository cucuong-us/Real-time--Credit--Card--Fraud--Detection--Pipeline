import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql.functions import from_json, col, regexp_replace, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession


current_dir = Path(__file__).parent
env_path = current_dir.parent / '.env'
load_dotenv(dotenv_path=env_path)


spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .getOrCreate()
# 1. Định nghĩa cấu trúc (Schema) của JSON giao dịch
schema = StructType([   
    StructField("User", IntegerType(), True),
    StructField("Card", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", DoubleType(), True),
    StructField("MCC", IntegerType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])
BRONZE_OUTPUT_PATH= os.getenv('BRONZE_OUTPUT_PATH')
BRONZE_RATES_PATH = os.getenv('BRONZE_RATES_PATH')
SILVER_FRAUD_OUTPUT_PATH= os.getenv('SILVER_FRAUD_OUTPUT_PATH')
SILVER_SAFE_OUTPUT_PATH= os.getenv('SILVER_SAFE_OUTPUT_PATH')
CHECKPOINT_SILVER_FRAUD_PATH= os.getenv('CHECKPOINT_SILVER_FRAUD_PATH')
CHECKPOINT_SILVER_SAFE_PATH= os.getenv('CHECKPOINT_SILVER_SAFE_PATH')

latest_rate_row = spark.read.format('delta').load(BRONZE_RATES_PATH).orderBy(col('update_at').desc()).limit(1).collect()
usd_to_vnd_rate = float(latest_rate_row[0]['rate'])
# 2. Đọc dữ liệu từ tầng Bronze
bronze_df = spark.readStream.format("delta").load(BRONZE_OUTPUT_PATH)
# 3. Xử lý làm sạch (Silver Transformation)
silver_df = bronze_df \
    .withColumn("jsonData", from_json(col("transaction_data"), schema)) \
    .select("jsonData.*") \
    .withColumnRenamed("Is Fraud?", "is_fraud") \
    .withColumnRenamed("Use Chip", "use_chip") \
    .withColumnRenamed("Merchant Name", "merchant_name") \
    .withColumnRenamed("Merchant City", "merchant_city") \
    .withColumnRenamed("Merchant State", "merchant_state") \
    .withColumn("Amount_USD", regexp_replace(col("Amount"), "\$", "").cast("double")) \
    .withColumn("Amount_VND", col("Amount_USD") * lit(usd_to_vnd_rate)) \
    .drop("Amount") # Bỏ cột 'Amount' gốc (dạng string $) vì đã có 'Amount_USD' và 'Amount_VND'
fraud_df = silver_df.filter(col("Is Fraud?") == "Yes")

# 2. Lọc ra các giao dịch an toàn
safe_df = silver_df.filter(col("Is Fraud?") == "No")

# 3. Ghi luồng GIAN LẬN vào một folder riêng
fraud_query = fraud_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_SILVER_FRAUD_PATH) \
    .start(SILVER_FRAUD_OUTPUT_PATH)

# 4. Ghi luồng AN TOÀN vào một folder riêng
safe_query = safe_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_SILVER_SAFE_PATH) \
    .start(SILVER_SAFE_OUTPUT_PATH)

print("🚀 Hệ thống đang phân loại dữ liệu thành 2 luồng riêng biệt!")