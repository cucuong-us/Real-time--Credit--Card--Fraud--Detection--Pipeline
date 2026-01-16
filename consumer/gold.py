import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql.functions import from_json, col, regexp_replace, lit, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession


current_dir = Path(__file__).parent
env_path = current_dir.parent / '.env'
load_dotenv(dotenv_path=env_path)


spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .getOrCreate()
SILVER_FRAUD_OUTPUT_PATH= os.getenv('SILVER_FRAUD_OUTPUT_PATH')
SILVER_SAFE_OUTPUT_PATH= os.getenv('SILVER_SAFE_OUTPUT_PATH')
CHECKPOINT_SILVER_FRAUD_PATH= os.getenv('CHECKPOINT_SILVER_FRAUD_PATH')
CHECKPOINT_SILVER_SAFE_PATH= os.getenv('CHECKPOINT_SILVER_SAFE_PATH')
# 1. Đọc dữ liệu từ folder fraud ở tầng Silver
fraud_silver_df = spark.readStream.format("delta").load(SILVER_FRAUD_OUTPUT_PATH)

# 2. Tính tổng tiền và số lượng vụ gian lận theo từng loại thẻ (card_id)
gold_fraud_stats = fraud_silver_df \
    .groupBy("Card") \
    .agg(
        sum("Amount").alias("total_fraud_amount"),
        count("Card").alias("total_fraud_cases")
    )

# 3. Ghi kết quả vào tầng Gold (Dùng OutputMode "complete" để cập nhật bảng tổng hợp)
gold_query_1 = gold_fraud_stats.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/mnt/gold/_checkpoints/fraud_stats_by_card") \
    .option("overwriteSchema", "true") \
    .start("/mnt/gold/fraud_stats_by_card")

