# Databricks notebook source
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pyspark.sql import Row

def get_usd_price_web_simple():
    # Giả lập trình duyệt để không bị chặn
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    url = "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
    
    response = requests.get(url, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, "lxml")
    
    # Tìm bảng tỷ giá
    rows = soup.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 5 and "USD" in cols[0].get_text():
            # Lấy giá mua tiền mặt, xóa dấu phẩy
            buy_cash = cols[2].get_text(strip=True).replace(',', '')
            return float(buy_cash)
    return None

def get_usd_rate_api():
    url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
    response = requests.get(url, timeout=10)
    return float(response.json()['usd']['vnd'])

# --- LOGIC CHÍNH TRÊN DATABRICKS ---
try:
    print("Đang thử lấy tỷ giá bằng API...")
    rate = get_usd_rate_api()
except Exception as e:
    print(f"API lỗi ({e}), đang chuyển sang cào Web Vietcombank...")
    rate = get_usd_price_web_simple()

if rate:
    df = spark.createDataFrame([Row(currency="USD", rate=rate, updated_at=datetime.now())])
    df.write.format("delta").mode("append").save("/mnt/bronze/exchange_rates")
    print(f"✅ Đã lưu tỷ giá: {rate}")
else:
    raise Exception("❌ Cả API và Web Scraping đều thất bại!")