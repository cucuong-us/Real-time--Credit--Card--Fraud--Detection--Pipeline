# 💳 Real-time Credit Card Fraud Detection Pipeline
### *End-to-End Financial Data Lakehouse on Azure Databricks*

[![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://www.python.org/)
[![DeltaLake](https://img.shields.io/badge/Delta%20Lake-00ADFF?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)

---

## 📌 Overview

This project implements a **real-time credit card fraud detection pipeline** using **Apache Spark Structured Streaming** on **Azure Databricks**.  
The system follows the **Medallion Architecture (Bronze – Silver – Gold)** to ensure data quality, scalability, and analytical performance.

Key capabilities:
- Real-time transaction ingestion
- Dynamic USD → VND currency conversion
- Fraud detection and segregation
- Low-latency analytics using Delta Lake

---

## 🏗 System Architecture

### Data Sources
- **Python Producer**: Streams simulated credit card transactions from CSV
- **Exchange Rate Crawler**: Crawls real-time USD/VND rates using Airflow

### Ingestion & Orchestration
- **Azure Event Hubs**: High-throughput streaming ingestion
- **Apache Airflow**: Pipeline orchestration and scheduling

### Processing & Storage
- **Azure Databricks**: Spark Structured Streaming engine
- **ADLS Gen2**: Delta Lake storage for Bronze, Silver, Gold layers

### Consumption
- **Databricks SQL** & **Power BI** for real-time dashboards

![Architecture Diagram](https://github.com/user-attachments/assets/73f3df01-b0bc-46dc-98b2-30e083399b94)

---

## 🛠 Tech Stack

| Layer | Technology |
|------|-----------|
| Languages | Python, SQL |
| Streaming | Spark Structured Streaming |
| Storage | Delta Lake (ACID, Time Travel) |
| Orchestration | Apache Airflow |
| Cloud | Azure (Event Hubs, ADLS Gen2, Databricks) |
| Containerization | Docker |

---

## 📂 Medallion Architecture

### 🥉 Bronze Layer – Raw Data
- Ingests raw JSON transactions from Event Hubs
- Stores exchange rate snapshots
- Append-only for audit & replay

### 🥈 Silver Layer – Cleaning & Enrichment
- Schema enforcement & type casting
- Real-time join with latest exchange rate
- Converts amounts from **USD to VND**
- Splits data into:
  - `safe_transactions`
  - `fraud_transactions`

### 🥇 Gold Layer – Analytics
- Hourly transaction volume aggregation
- Fraud loss analysis by Card ID & Merchant Category
- Optimized Delta tables for BI tools

---

## 🚀 Deployment Guide

### 1. Prerequisites
- Active **Azure Subscription**
- Azure Event Hubs, ADLS Gen2, Azure Databricks
- Docker & Docker Compose (Linux/Ubuntu)
- Python 3.9+

---

### 2. Start Local Services

```bash
sudo docker compose up
```

Access Web UI: Open your browser and navigate to:
```bash
http://localhost:8085
```
Authentication:

Username: `admin`

Password: `admin`

Triggering DAGs: Locate the exchange_rate_crawler DAG and toggle it to On. This will start fetching live currency data and feeding it into your Bronze layer.

### 3. Databricks Configuration & Execution
There are two ways to deploy and run the notebooks on your Databricks cluster:

#### **Option 1: Using Databricks Web UI (Standard)**
1. **Cluster Setup**: Create a Databricks cluster (Runtime 13.x+ recommended).
2. **Install Libraries**: 
   - Go to the **Libraries** tab on your cluster.
   - Install a new library via **Maven**: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`.
3. **Import Notebooks**: 
   - Upload the `.ipynb` files directly to your Workspace.
   - Configure your `connectionString` and SAS Key in the first cell.
4. **Execution**: Run the notebooks in order: `Bronze` ➡️ `Silver` ➡️ `Gold`.

#### **Option 2: Using VS Code (Modern Developer Workflow)**
1. **Extension**: Install the **Databricks Extension for Visual Studio Code**.
2. **Connection**: 
   - Authenticate your VS Code with your Databricks Workspace using a **Personal Access Token (PAT)**.
   - Select your active cluster within the extension.
3. **Library Check**: Ensure the Maven library mentioned in Option 1 is already installed on the cluster via Web UI.
4. **Execution**: 
   - Open the local `.ipynb` files.
   - Click **Run on Databricks** to execute the logic of `Bronze.py`, `Silver.py`, and `Gold.py` sequentially.

---

#### 🛠 Configuration Details
Before running, ensure the following parameters are updated in your notebooks/scripts:

* **Event Hubs**: `connectionString` & `SharedAccessKey`.
* **Storage**: Verify the following mount paths are accessible:
    - `/mnt/bronze`
    - `/mnt/silver`
    - `/mnt/gold`
---

## 📊 Key Results

- ⚡ **Low Latency**: End-to-end processing delay of under **5 seconds**.
- 🚀 **High Throughput**: Capable of processing over **1,000,000 transactions per day** with stable performance.
- 🔄 **Idempotency**: Robust checkpointing ensures the stream recovers without data loss or duplication.
- 📈 **Real-time Visualization**: Dashboards automatically refresh as new transaction batches are processed.
---

## 👤 Author

**Đoàn Cường – Data Engineer**

📅 **Project Completed**: January 2026

