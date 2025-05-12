# TCGA DNA Methylation & Clinical Metadata ELT Pipeline

This repository contains the end-to-end ELT (Extract, Load, Transform) pipeline for ingesting, processing, and preparing TCGA DNA methylation and clinical metadata for machine learning tasks. The pipeline is orchestrated with Apache Airflow, stores raw data on Google Cloud Storage (GCS), and uses Google BigQuery for data warehousing and transformation with `dbt`.

---

## 📊 Pipeline Overview

```
GDC Portal → Apache Airflow (Extract) → GCS → BigQuery (Load) → dbt SQL Models (Transform) → ML-Ready Tables
```

---

## ⚙️ Architecture: ELT

### 1. Extract

* **Source:** [GDC Data Portal](https://portal.gdc.cancer.gov/)
* **Orchestration:** Apache Airflow DAG
* **Tools Used:**

  * `gdc-client` for secure data downloads
  * Checksum validation for data integrity
* **Storage:**

  * Google Cloud Storage with versioned paths
  * Path format: `gs://tcga-raw/vYYYYMMDDHHMMSS/`
* **Tradeoffs:**

  * Higher setup complexity vs automated, reliable ingestion
  * Increased storage vs auditability and reproducibility

### 2. Load

* **Destination:** Google BigQuery staging tables
* **Data Types:**

  * Methylation data
  * Clinical metadata (JSON/CSV)
* **Version Control:**

  * GCS versioned buckets for raw files
  * [DVC](https://dvc.org/) to link processing code with input manifests
* **Tradeoffs:**

  * Raw data storage overhead vs analytical flexibility
  * Dual versioning vs end-to-end reproducibility

### 3. Transform

* **Engine:** [`dbt`](https://www.getdbt.com/) SQL transformations within BigQuery
* **Features:**

  * Data cleansing (null handling, schema drift checks)
  * Normalization (e.g., quantile normalization via SQL UDFs)
  * Feature engineering (window functions, aggregate stats)
* **Validation:**

  * dbt tests for null checks and schema consistency
* **Tradeoffs:**

  * SQL-based logic is limited vs high accessibility and performance
  * Basic imputation in SQL vs advanced ML-based imputation outside the warehouse

---

### Prerequisites

* Python 3.8+
* Google Cloud SDK
* BigQuery and GCS access
* Apache Airflow setup
* dbt CLI

---

## 🧪 Future Work

* Integrate advanced imputation methods using BigQuery ML or TensorFlow
* Add support for more omics layers (RNA-seq, mutation)
* Deploy on GCP Composer for managed Airflow

---
