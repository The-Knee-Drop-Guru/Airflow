![Python 3.8](https://img.shields.io/badge/Python-3.8-blue)
![Completed](https://img.shields.io/badge/Status-Completed-green)
![License](https://img.shields.io/badge/License-All%20Rights%20Reserved-blue)
# MLOps Pipeline<br>for BTC Price Prediction Using Apache Airflow
## Overview
This repository contains an MLOps pipeline built with Apache Airflow.<br>
This pipeline is part of the project "Building a BTC Price Prediction & Analytics Platform".<br>
It was developed by **Team The-Knee-Drop-Guru**,<br>as part of the Programmers Data Engineering Course (4th cohort).<br>
The goal of this project is to build an MLOps pipeline that predicts the BTC price daily<br>using the Binance and Yahoo Finance APIs and deploys it to a web application for analytics.

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
1. **Data Sources**: 
   - **Binance & Yahoo Finance** APIs provide cryptocurrency and stock market data.

2. **Daily ETL Process**:
   - Managed by **Apache Airflow**.
   - Extracts, transforms, and loads raw data into **AWS S3** on a daily basis.

3. **Daily Training Process**:
   - Managed by **Apache Airflow**.
   - Preprocesses the data, trains the ML model, and predicts the daily BTC closing price.
   - Triggered daily, immediately after the ETL process.

---

## Features
- Fully Automated ETL and Machine Learning Training for Daily Predictions.
- Modularized Functions for Easy Reuse and Modification.
- Easily Deployable Using Python Virtual Environment.

---

## Project Main Structure
```
.
├── dags
│   ├── extract_batch_dag.py                # DAG for extracting raw data daily for training
│   ├── extract_binance_hourly_dag.py       # DAG for extracting raw data hourly for real-time price visualization
│   └── training_dag.py                     # DAG for daily continuous training
└── plugins
    ├── fetch_data                          # Functions for fetching raw data from various APIs
    │   ├── fetch_binance_batch.py
    │   ├── fetch_binance_hourly.py
    │   ├── fetch_news_daily.py
    │   ├── fetch_reddit_daily.py
    │   └── fetch_yahoo_batch.py
    ├── train                               # Functions for daily continuous training
    │   ├── preprocessing.py
    │   └── train_n_predict.py
    ├── upload_data                         # Functions for transporting data to databases or storage
    │   ├── download_from_s3.py
    │   ├── insert_to_db_daily.py
    │   ├── insert_to_db_hourly.py
    │   └── upload_to_s3.py
    └── utils                               # Functions for data manipulation
        └── data_window.py                      
```

---

## Contributing
Chan-Song Kwon, Min-Gyu Kim, Yoong-Jung Kim, Soo-Min Jeon
