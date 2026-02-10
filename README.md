# 🏎️ Formula 1 End-to-End Data Pipeline

<div align="center">

<a href="https://www.microsoft.com/en-us/microsoft-fabric" target="_blank"><img src="https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white" alt="Microsoft Fabric"/></a>&nbsp;<a href="https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction" target="_blank"><img src="https://img.shields.io/badge/OneLake_(ADLSg2)-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" alt="OneLake"/></a>&nbsp;<a href="https://spark.apache.org/" target="_blank"><img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"/></a>&nbsp;<a href="https://www.python.org/" target="_blank"><img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/></a>&nbsp;<a href="https://delta.io/" target="_blank"><img src="https://img.shields.io/badge/Delta_Lake-333333?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake"/></a>&nbsp;<a href="https://openf1.org/" target="_blank"><img src="https://img.shields.io/badge/OpenF1_API-FF1801?style=for-the-badge&logo=formula1&logoColor=white" alt="OpenF1"/></a>

</div>

<img width="2248" height="1132" alt="image" src="https://github.com/user-attachments/assets/6ec220b7-eff4-4144-ba27-fa1267ca2f62" />
<table style="width: 100%; border-collapse: collapse; border: none;">
  <tr style="border: none;">
    <td style="width: 50%; border: none; padding: 5px;">
      <img src="https://github.com/user-attachments/assets/a01df9ce-a4b4-4aaa-92cf-7857999c3fcb" alt="Drivers Dashboard" style="width: 100%;">
      <p align="center"><b>Teams Dashboard</b></p>
    </td>
    <td style="width: 50%; border: none; padding: 5px;">
      <img src="https://github.com/user-attachments/assets/2bed21d4-01cc-4a78-856e-b1bf1ccfb72b" alt="Teams Dashboard" style="width: 100%;">
      <p align="center"><b>Drivers Dashboard</b></p>
    </td>
  </tr>
</table>


## 📖 Project Overview
This personal project aims to build a complete **End-to-End Data Pipeline** using **Microsoft Fabric**. The system ingests historical Formula 1 data, processes it following the Medallion Architecture (Bronze, Silver, Gold), and serves it for analytical reporting.

The main goal is to apply **DP-600 (Fabric Analytics Engineer)** certification concepts in a real-world scenario.

## 🏗️ Architecture (Medallion)
The data flows from the API through a structured Lakehouse architecture:

<div align="center">
  <img src="assets/mermaid-architecture_medallion_v2.png" alt="Architecture Diagram" width="800"/>
</div>

* **Bronze (Raw):** Landing zone for raw JSON data from the API.

* **Silver (Cleaned):** Data is deduplicated, typed, and stored as Delta Parquet tables.

* **Gold (Curated):** Business-level aggregations and Dimensional Modeling (Star Schema).

## 🛠️ Tech Stack
* **Cloud Platform:** Microsoft Fabric (Data Engineering & Data Warehouse).

* **Storage:** OneLake (ADLS Gen2).

* **Compute:** Spark Pools (PySpark) & T-SQL.

* **Orchestration:** Fabric Data Factory Pipelines.

* **Source:** OpenF1 API.

## 🚀 Roadmap & Progress
- [x] **Phase 1: Environment Setup (Fabric + GitHub)**

- [x] **Phase 2: Ingestion (Bronze)**
    - [x] Drivers Data
    - [x] Constructors Data
    - [x] Circuits Data

- [x] **Phase 3: Silver Layer Transformation (Data Cleaning)**
    - [x] Drivers Data
    - [x] Constructors Data
    - [x] Circuits Data
    
- [x] **Phase 4: Gold Layer Modeling (Dimensions & Facts)**
    - [x] Drivers Data
    - [x] Constructors Data
    - [x] Circuits Data

- [x] **Phase 5: Final Dashboard in Power BI**
    - [x] Driver Championship Data
    - [x] Constructor Championship Data

## 💻 How to Run & Deploy

This project is built to run natively inside a **Microsoft Fabric Workspace**.

### 1. Environment Setup
1.  Create a new **Fabric Workspace** (with Trial or Capaciity enabled).
2.  Go to **Workspace Settings > Git Integration**.
3.  Connect this repository and click **Sync** or **Connect**. This will automatically import all Notebooks and Pipelines.

### 2. Data Ingestion (Orchestration)
Instead of running notebooks manually, trigger the master pipeline to handle dependencies:
1.  Open the pipeline named **`Data Pipe`**.
2.  Click **Run**.
    * *This will execute the Bronze (Ingestion) -> Silver (Transformation) -> Gold (Modeling) notebooks in sequence.*

### 3. Visualization
1.  **Open the F1_Gold_Model (Semantic Model) in your workspace.**
2.  **Go to Settings and ensure it is pointing to your newly created lh_f1 Lakehouse.**
3.  **Open the F1_Data_Analytics_Platform (Report).**

## 📂 Project Structure
```text
f1-fabric-proyect/
│
├── 📂 assets/                  # Static resources for UI & Documentation
│   ├── 📂 teams/               # Team logos and assets for Power BI
│   └── 📂 screenshots/         # Images used in this README
│
├── 📂 src/                     # Source Code (Fabric Notebooks & ETL)
│
├── .gitignore                  # Git configuration
└── README.md                   # Project Documentation
