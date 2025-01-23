![Python 3.8](https://img.shields.io/badge/Python-3.8-blue)
![Completed](https://img.shields.io/badge/Status-Completed-green)
![License](https://img.shields.io/badge/License-All%20Rights%20Reserved-blue)
# MLOps Pipeline<br>for "BTC Price Prediction & Analytics Platform"<br>Using Apache Airflow
## Overview
This repository contains a MLOps pipeline using Apache Airflow.<br>The pipeline is for the project "Building a BTC Price Prediction & Analytics Platform".<br>It is developed by **Team The-Knee-Drop-Guru**, as part of the Programmers' Data Engineering Course(4th).<br>The aim of this project is to build a MLOps pipeline that predicts the BTC price daily<br>using the Binance & Yahoo Finance API and deploy it on a web application for analytics.

---

## Project Demo (Click ↓ to Play)
<!-- img size -->
<div align="center">
<a href="https://www.youtube.com/watch?v=Pyh8T3MzuS8">
    <img src="https://img.youtube.com/vi/Pyh8T3MzuS8/0.jpg" width="350" />
</a>
</div>

---

## Architecture

The pipeline is designed as follows:
1. **Data Source**: 
   - **CoinAPI.io**: Serves as the data provider, offering cryptocurrency market data via API.

2. **ETL Process**:
   - Managed by **Apache Airflow**.
   - Extracts, transforms, and loads raw cryptocurrency data into **Snowflake**.

3. **ELT Process**:
   - Powered by **dbt** (Data Build Tool).
   - Transforms staged data in Snowflake, runs models, and outputs analytics-ready tables.

4. **Visualization**:
   - **Apache Superset** visualizes the processed data to provide actionable insights.

5. **Containerization**:
   - Entire pipeline is packaged using **Docker** for consistent and reproducible deployments.

---

## Features
- Machine learning model for price prediction
- Web app deployment for real-time predictions

---

## Project Main Structure
```
.
├── dags
│   ├── extract_batch_dag.py                #
│   ├── extract_binance_hourly_dag.py
│   └── training_dag.py
└── plugins
    ├── fetch_data
    │   ├── fetch_binance_batch.py
    │   ├── fetch_binance_hourly.py
    │   ├── fetch_news_daily.py
    │   ├── fetch_reddit_daily.py
    │   └── fetch_yahoo_batch.py
    ├── train
    │   ├── preprocessing.py
    │   └── train_n_predict.py
    ├── upload_data
    │   ├── download_from_s3.py
    │   ├── insert_to_db_daily.py
    │   ├── insert_to_db_hourly.py
    │   └── upload_to_s3.py
    └── utils
        └── data_window.py
```
---

## Workflow

1. **Data Extraction**: Apache Airflow schedules and executes tasks to fetch raw data from CoinAPI.io.
2. **Data Loading**: Transformed data is loaded into Snowflake.
3. **Data Transformation**: dbt models refine the data into analytics-ready tables.
4. **Visualization**: Apache Superset visualizes metrics, trends, and insights.

---

## Contributing
Sadhvi Singh, Varshini Rao
