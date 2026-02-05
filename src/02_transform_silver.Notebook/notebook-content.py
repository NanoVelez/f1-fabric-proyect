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

# --- CELL 1: DRIVERS TRANSFORMATION ---
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

# CELL ********************

# --- CELL 2: TEAMS TRANSFORMATION (CORRECTED) ---

# --- CONFIGURATION ---
INPUT_PATH_TEAMS = f"Files/bronze/{YEAR}/*/teams_standings.json"
TABLE_NAME_TEAMS = "silver_teams_standings"

# --- 1. READ ---
print(f"Reading Teams files from: {INPUT_PATH_TEAMS}...")
df_teams_raw = spark.read.json(INPUT_PATH_TEAMS)

# --- 2. TRANSFORMATION ---
print("Transforming Teams data...")

df_teams_silver = df_teams_raw.select(
    # A. Business Columns
    col("position_current").cast("int").alias("position"),
    col("points_current").cast("float").alias("points"),
    col("team_name"),  # Este será tu identificador principal
    # col("team_key").cast("long"),  <-- LINEA ELIMINADA PORQUE NO EXISTE EN EL JSON
    col("session_key").cast("long"),
    col("meeting_key").cast("long"),
    
    # B. Data Engineering
    element_at(split(input_file_name(), "/"), -2).alias("race_folder_name"),
    current_timestamp().alias("ingestion_date"),
    lit(YEAR).alias("season_year")
)

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME_TEAMS}...")
df_teams_silver.orderBy("race_folder_name", "position") \
    .write.mode("overwrite").format("delta").saveAsTable(TABLE_NAME_TEAMS)

print("Teams table created successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 3: CIRCUITS TRANSFORMATION (FILTERED) ---

# --- CONFIGURATION ---
INPUT_PATH_CIRCUITS = f"Files/bronze/{YEAR}/circuits.json"
TABLE_NAME_CIRCUITS = "silver_circuits"

# --- 1. READ ---
print(f"Reading Circuits master file from: {INPUT_PATH_CIRCUITS}...")
df_circuits_raw = spark.read.json(INPUT_PATH_CIRCUITS)

# --- 2. TRANSFORMATION ---
print("Transforming Circuits data...")

df_filtered = df_circuits_raw.filter(~col("meeting_name").contains("Testing"))

df_circuits_silver = df_filtered.select(
    col("meeting_key").cast("long"),
    col("meeting_name").alias("gp_name"),
    col("location").alias("city"),
    col("country_name").alias("country"),
    col("circuit_short_name").alias("circuit"),
    col("date_start").cast("timestamp"),
    col("year").cast("int"),
    current_timestamp().alias("ingestion_date")
)

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME_CIRCUITS}...")
df_circuits_silver.orderBy("date_start") \
    .write.mode("overwrite").format("delta").saveAsTable(TABLE_NAME_CIRCUITS)

print("Circuits table created successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 4: DRIVERS DIMENSION ---

# --- CONFIGURATION ---
INPUT_PATH_DRIVERS = f"Files/bronze/{YEAR}/*/drivers.json"
TABLE_NAME_DRIVERS = "silver_drivers"

# --- 1. READ ---
print(f"Reading Drivers catalog from: {INPUT_PATH_DRIVERS}...")
df_drivers_raw = spark.read.json(INPUT_PATH_DRIVERS)

# --- 2. TRANSFORMATION ---
print("Creating unique drivers list...")

df_drivers_silver = df_drivers_raw.select(
    col("driver_number").cast("int"),
    col("full_name"),
    col("name_acronym"),
    col("team_name"),       
    col("headshot_url"),    
    col("country_code"),
    current_timestamp().alias("ingestion_date")
).dropDuplicates(["driver_number"]) 

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME_DRIVERS}...")
df_drivers_silver.write.mode("overwrite").format("delta").saveAsTable(TABLE_NAME_DRIVERS)

print("Drivers Dimension created successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
