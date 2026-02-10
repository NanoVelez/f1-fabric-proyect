# F1 Data Analytics Platform | Microsoft Fabric End-to-End

<div align="center">

<a href="https://www.microsoft.com/en-us/microsoft-fabric" target="_blank"><img src="https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white" alt="Microsoft Fabric"/></a>&nbsp;<a href="https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction" target="_blank"><img src="https://img.shields.io/badge/OneLake_(ADLSg2)-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" alt="OneLake"/></a>&nbsp;<a href="https://spark.apache.org/" target="_blank"><img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"/></a>&nbsp;<a href="https://www.python.org/" target="_blank"><img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/></a>&nbsp;<a href="https://delta.io/" target="_blank"><img src="https://img.shields.io/badge/Delta_Lake-333333?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake"/></a>&nbsp;<a href="https://openf1.org/" target="_blank"><img src="https://img.shields.io/badge/OpenF1_API-FF1801?style=for-the-badge&logo=formula1&logoColor=white" alt="OpenF1"/></a>

</div>

<img width="2248" height="1132" alt="image" src="assets/screenshots/Main_Menu.png" />
<table style="width: 100%; border-collapse: collapse; border: none;">
  <tr style="border: none;">
    <td style="width: 50%; border: none; padding: 5px;">
      <img src="assets/screenshots/Team_Standings.png" alt="Drivers Dashboard" style="width: 100%;">
      <p align="center"><b>Teams Dashboard</b></p>
    </td>
    <td style="width: 50%; border: none; padding: 5px;">
      <img src="assets/screenshots/Driver_Standings.png" alt="Teams Dashboard" style="width: 100%;">
      <p align="center"><b>Drivers Dashboard</b></p>
    </td>
  </tr>
</table>


## Project Overview
This personal project aims to build a complete **End-to-End Data Pipeline** using **Microsoft Fabric**. The system ingests historical Formula 1 data, processes it following the Medallion Architecture (Bronze, Silver, Gold), and serves it for analytical reporting.

The main goal is to apply **DP-700 (Fabric Data Engineer)** certification concepts in a real-world scenario.

## Architecture (Medallion)
The data flows from the API through a structured Lakehouse architecture:

<div align="center">
  <img src="assets/mermaid-architecture_medallion_v2.png" alt="Architecture Diagram" width="800"/>
</div>

* **Bronze (Raw):** Landing zone for raw JSON data from the API.

* **Silver (Cleaned):** Data is deduplicated, typed, and stored as Delta Parquet tables.

* **Gold (Curated):** Business-level aggregations and Dimensional Modeling (Star Schema).

## Tech Stack
* **Cloud Platform:** Microsoft Fabric (Data Engineering & Data Warehouse).

* **Storage:** OneLake (ADLS Gen2).

* **Compute:** Spark Pools (PySpark) & T-SQL.

* **Orchestration:** Fabric Data Factory Pipelines.

* **Source:** OpenF1 API.

## Roadmap & Progress
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
     
---

## Deep Dive: Architecture & Business Logic

### 1. Ingestion & Orchestration (The Pipeline)
The workflow begins with the total automation of the data lifecycle. This is not just a collection of scripts; it is a resilient process orchestrated natively within **Microsoft Fabric**.

* **Data Factory Orchestration:** A master pipeline manages dependencies, ensuring the **Silver** process only triggers if **Bronze** succeeds, maintaining referential integrity from the start.
* **API Limit Handling:** The ingestion layer is built to handle paginated requests and respect rate limits from the OpenF1 API, preventing data loss or connection blocks.

<img width="1947" height="444" alt="image" src="https://github.com/user-attachments/assets/a153db86-bebd-4152-95b7-431c28b546b4" />

* **Dynamic Parameterization (Variables & Parameters):** To achieve full automation and multi-season support, I implemented a **Parameter-Driven Architecture**. 
    * **Automated Yearly Ingestion:** Instead of hard-coding values, the pipeline uses **Pipeline Variables** to store the `Year`. 
    * **Runtime Injection:** These variables are passed as parameters to the Notebook activities at runtime. This allows the system to dynamically adjust API endpoints and folder paths (e.g., `Files/Bronze/2024/`) without manual intervention.
    * **Scalability:** By simply changing a single pipeline-level parameter, the entire Medallion flow can be re-run for any F1 season, making the project a reusable framework rather than a static analysis.

---

### 2. Silver Layer: Data Quality & "Edge Cases"
This is where the engineering value shines. F1 data is notoriously chaotic—drivers switch teams, races get cancelled, and teams undergo rebranding.

####  Team-Switch Proof (Mid-Season Transfers)
* **The Problem:** A driver (e.g., Ricciardo/Lawson in 2023) can change teams mid-year. A simple JOIN on `Driver_Key` would duplicate records or lose historical context.
* **The Solution:** I implemented a **Dynamic Normalization** logic. Results are bound to a unique combination of `Race_Key` + `Driver_Key` + `Team_Key`. This ensures a driver's history remains intact regardless of the colors they wore in a specific Grand Prix.

####  Custom Data Enrichment:
* **The Problem:** The OpenF1 API occasionally lacks standardized short codes for Grand Prix names, which are essential for clean visualizations and axis labels.
* **The Solution:** I engineered a custom mapping logic to generate consistent **3-letter Alpha Codes** (e.g., `MON` for Monaco, `SPA` for Spain). 
* **Impact:** This enrichment ensures that drivers charts remain visually consistent and readable, regardless of the raw naming conventions provided by the source API.

####  Data Cleansing & Deduplication
* Leveraging **PySpark** `dropDuplicates()` functions based on sensor timestamps to ensure the Silver layer only stores a "Single Source of Truth," even if the source API sends redundant telemetry.

---

### 3. Gold Layer: Star Schema Modeling
The semantic model is optimized for **Direct Lake** mode, eliminating the need for manual refreshes and providing sub-second performance directly over OneLake.

* **Fact Tables:** Store granular metrics such as race positions, points, and lap times.
* **Dimension Tables:** Descriptive attributes for drivers, teams, circuits, and the racing calendar.
* **Relationships:** Protected 1:N relationships prevent ambiguity; filtering by a Team correctly displays every driver who has competed for that constructor.

<img width="1684" height="931" alt="image" src="https://github.com/user-attachments/assets/7ecaec97-5f71-419c-8b89-ef36f20c5f1e" />


---

### 4. Advanced DAX: The "Memory Effect"
For a professional-grade dashboard, trend lines must remain continuous even if a team lacks data for a specific GP or a race is cancelled.

```dax
-- Logic for Team Ranking Persistence (Forward Fill)
Team_Ranks = 
CALCULATE(
    LASTNONBLANKVALUE(
        gold_dim_race[Date],
        MAX(gold_fact_team_results[World_Position])
    ),
    REMOVEFILTERS(gold_dim_race),
    gold_dim_race[Date] <= MAX(gold_dim_race[Date]),
    VALUES(gold_dim_race[Year]) -- Prevents historical team overlap
)
```
---

### 5. Visualization & UX (User Experience)
The report is designed for high-level competitive analysis, moving beyond simple data tables.

#### Deneb & Vega-Lite:
* Implementation of custom visuals to display time densities and dynamic rankings.

####  Interactive Season Timeline
* To provide a truly immersive experience, I implemented a **Dynamic Timeline** that allows you to "replay" the season.

![Timeline](https://github.com/user-attachments/assets/74604975-9669-4700-9758-ba6250a96fd6)



* **Temporal Storytelling:** The timeline doesn't just filter data; it recalculates the entire state of the championship at any given point in time.

#### Driver Championship Standings
* **Multi-Season & Round-by-Round Navigation:** Users can slice data by specific seasons or replay the championship race-by-race.
* The view below provides a deep dive into individual performance, tracking the evolution of the title race with a focus on consistency and head-to-head metrics.

<img width="2048" height="1140" alt="image" src="https://github.com/user-attachments/assets/557fac85-161e-4cbb-a41e-276d6a9c43de" />

---

## Future Enhancements

To further evolve this platform from a descriptive dashboard to a predictive analytics tool, the following features are planned:

* **Real-Time Telemetry Integration:** Incorporating live signal data (speed, throttle, brake) using **Fabric Real-Time Intelligence** to analyze driver performance during live sessions.
* **Predictive Modeling:** Implementing **Synapse ML** to predict pit stop windows and race finishing positions based on historical degradation and weather variables.
* **Advanced Tyre Strategy Analysis:** Developing a dedicated Gold layer table to track stint lengths and compound performance for more granular strategy insights.

---

## How to Run & Deploy

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

---



## 📂 Project Structure
```text
f1-fabric-proyect/
│
├── 📂 assets/                  # Static resources for UI & Documentation
│   ├── 📂 teams/               # Team logos and assets for Power BI
│   └── 📂 screenshots/         # Images used in this README
├── 📂 src/                     # Source Code (Fabric Notebooks & ETL)
├── .gitignore 
├── LICENSE                     # Git configuration
└── README.md                   # Project Documentation
