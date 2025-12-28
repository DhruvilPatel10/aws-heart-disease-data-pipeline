# AWS Heart Disease Data Pipeline & Analytics

An end-to-end **big data analytics pipeline** built on **AWS** to analyze and visualize heart disease risk factors using large-scale survey data.  
This project demonstrates **data lake architecture**, **ETL with AWS Glue**, **distributed processing with Apache Spark**, and **analytics-driven visualization**.

---

## 📌 Project Goal

To evaluate how effectively survey responses can be used to:
- Predict heart disease risk
- Identify high-risk combinations of health indicators
- Support preventative health screening and decision-making

---

## 🧠 Dataset

- **Source:** Kaggle – Heart Disease Health Indicators (BRFSS 2015)
- **Format:** CSV
- **Size:** Large-scale public health survey data
- **Target Variable:** `HeartDiseaseorAttack`

Dataset link:  
https://www.kaggle.com/datasets/alexteboul/heart-disease-health-indicators-dataset

---

## 🏗️ Architecture Overview

This project follows a **Data Lake (Schema-on-Read)** architecture on AWS:

Amazon S3 (Raw Data)
↓
AWS Glue Crawler
↓
AWS Glue Data Catalog
↓
AWS Glue ETL Job
↓
Amazon S3 (Processed Data)
↓
Amazon EMR (Spark)
↓
Amazon S3 (Final Outputs & Visualizations)


---

## ☁️ AWS Services Used

- **Amazon S3** – Data Lake storage
- **AWS Glue Crawler** – Metadata discovery
- **AWS Glue ETL Job** – Data transformation
- **AWS Glue Data Catalog** – Centralized metadata repository
- **Amazon EMR** – Distributed Spark processing
- **Apache Spark (PySpark)** – Analytics & visualization
- **AWS IAM** – Secure access control

---

## 📁 Project Structure

AWS-heart-disease-data-pipeline/
├── heart_disease_health_indicators_BRFSS2015.csv # Raw dataset
├── heart_disease_glue_job.py # AWS Glue ETL script
├── spark_script.py # Spark EDA & count plots
├── sql_visualization.py # Spark SQL analytics
├── images/ # Generated visualizations
├── report.pdf # Final analytical report
├── train.csv # ML training data
├── test.csv # ML test data
├── prediction_code.ipynb # Prediction notebook
└── README.md

---

## 🔄 Data Pipeline Workflow

### 1️⃣ Data Ingestion
- Raw CSV uploaded to Amazon S3 (`finalproinput`)
- Schema applied at read-time (schema-on-read)

### 2️⃣ Metadata Extraction
- AWS Glue Crawler scans S3 bucket
- Metadata stored in AWS Glue Data Catalog

### 3️⃣ ETL Processing (AWS Glue)
- Data cleaned and transformed using PySpark
- Output written to S3 in compressed JSON format

### 4️⃣ Big Data Processing (Amazon EMR)
- Spark jobs process transformed data
- Supports parallel and distributed execution

---

## 📊 Exploratory Data Analysis & Visualization

### Spark-Based Analysis (`spark_script.py`)
- Reads processed data from S3
- Converts Spark DataFrame → Pandas DataFrame
- Generates:
  - Count plots
  - Box plots
- Uploads visualizations to S3

### Spark SQL Analytics (`sql_visualization.py`)
Performs SQL-based analysis for:
- **High-risk health combinations**
- **Healthy lifestyle impact**
- **Gender-based heart disease trends**

All plots are automatically saved and uploaded to S3.

---

## 📈 Sample Insights Generated

- Obesity, diabetes, and high cholesterol significantly increase heart disease risk
- Higher income + healthy diet + healthcare access correlates with lower risk
- Gender and age combinations reveal distinct risk patterns

---

## 🧪 Predictive Modeling (Optional Extension)

- Data split into training and testing sets
- Prediction notebook (`prediction_code.ipynb`) explores ML-based risk prediction
- Enables future integration with scalable ML pipelines

---

## 🎯 Key Learnings

- Designed a scalable **AWS-based data lake**
- Implemented **ETL using AWS Glue**
- Performed **distributed analytics using Spark**
- Built **automated visualization pipelines**
- Applied big data tools to real-world healthcare analytics

---

## 🚀 Future Enhancements

- Integrate AWS Athena for interactive querying
- Add ML models using Spark MLlib
- Automate pipeline with AWS Step Functions
- Deploy dashboards using Amazon QuickSight
