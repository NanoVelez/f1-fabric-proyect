# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "061b5888-c63f-4658-b28e-fffb5af3ca67",
# META       "default_lakehouse_name": "lh_f1",
# META       "default_lakehouse_workspace_id": "399cf811-13f0-4d3d-80bb-5f12b960d7a3",
# META       "known_lakehouses": [
# META         {
# META           "id": "061b5888-c63f-4658-b28e-fffb5af3ca67"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import input_file_name, col, split, element_at, current_timestamp, lit

# --- CONFIGURATION ---
YEAR = "2023"
INPUT_PATH = f"Files/bronze/{YEAR}/*/drivers_standings.json"
TABLE_NAME = "silver_drivers_standings"

# --- 1. READ ---
print(f"Reading files from: {INPUT_PATH}...")
df_raw = spark.read.json(INPUT_PATH)

# --- 2. TRANSFORMATION ---
print("Mapping real columns (API) to clean columns (Silver)...")

df_silver = df_raw.select(
    # A. Business Columns 
    col("position_current").cast("int").alias("position"),  # Rename on the fly
    col("points_current").cast("float").alias("points"),    # Rename on the fly
    col("driver_number").cast("int"),
    col("session_key").cast("long"),
    col("meeting_key").cast("long"),
    
    # B. Data Engineering
    element_at(split(input_file_name(), "/"), -2).alias("race_folder_name"),
    current_timestamp().alias("ingestion_date"),
    lit(YEAR).alias("season_year")
)

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME}...")
df_silver_sorted = df_silver.orderBy("race_folder_name", "position")
df_silver_sorted.write.mode("overwrite").format("delta").saveAsTable(TABLE_NAME)

print("Table created successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
