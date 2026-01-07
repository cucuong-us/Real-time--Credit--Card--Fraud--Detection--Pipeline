# 1. Dùng image Python nhẹ
FROM python:3.10-slim

# 2. Thiết lập thư mục làm việc trong container
WORKDIR /app

# 3. Copy file requirements và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy toàn bộ code vào container
COPY . .

# 5. Lệnh chạy chính
CMD ["python", "main.py"]