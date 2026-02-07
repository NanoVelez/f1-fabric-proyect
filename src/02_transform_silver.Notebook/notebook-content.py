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

# --- CELL 1: DRIVERS STANDINGS TRANSFORMATION (SCALABLE) ---

from pyspark.sql.functions import input_file_name, col, split, element_at, current_timestamp, lit

# --- CONFIGURATION ---
INPUT_PATH_ROOT = "Files/bronze/"
TABLE_NAME = "silver_drivers_standings"

# --- 1. READ ---
print(f"Reading ALL Drivers Standings files recursively...")

df_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "drivers_standings.json") \
    .json(INPUT_PATH_ROOT)

# --- 2. TRANSFORMATION ---
print("Mapping columns and Extracting Year/Race...")

df_silver = df_raw.select(
    # A. Business Columns 
    col("position_current").cast("int").alias("position"),  
    col("points_current").cast("float").alias("points"), 
    col("driver_number").cast("int"),
    col("session_key").cast("long"), 
    col("meeting_key").cast("long"),
    
    # B. Data Engineering
    element_at(split(input_file_name(), "/"), -2).alias("race_folder_name"),
    element_at(split(input_file_name(), "/"), -3).cast("int").alias("year"),
    
    current_timestamp().alias("ingestion_date")
).na.drop(subset=["driver_number", "position"])

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME}...")

df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME)

print(f"Drivers Standings table created! Total rows: {df_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 2: SILVER TEAMS STANDINGS (UPDATE) ---
from pyspark.sql.functions import col, split, element_at, current_timestamp, lit, regexp_replace, input_file_name, concat

# --- CONFIGURATION ---
INPUT_PATH_ROOT = "Files/bronze/"
TABLE_NAME_TEAMS = "silver_teams_standings"
GITHUB_ASSETS_URL = "https://raw.githubusercontent.com/NanoVelez/f1-fabric-proyect/main/assets/teams/"

# --- 1. READ ---
df_teams_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "teams_standings.json") \
    .json(INPUT_PATH_ROOT)

# --- 2. TRANSFORMATION ---
df_teams_silver = df_teams_raw.select(
    col("position_current").cast("int").alias("position"),
    col("points_current").cast("float").alias("points"),
    col("team_name"),
    col("meeting_key").cast("long"), 

    # Data Engineering
    element_at(split(input_file_name(), "/"), -2).alias("race_folder_name"),
    element_at(split(input_file_name(), "/"), -3).cast("int").alias("year"),
    current_timestamp().alias("ingestion_date")
).withColumn(
    "team_logo_url", 
    concat(
        lit(GITHUB_ASSETS_URL), 
        regexp_replace(col("team_name"), " ", "%20"), 
        lit(".png")
    )
).na.drop(subset=["team_name", "position"])

# --- 3. WRITE ---
df_teams_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME_TEAMS)

print("Silver Teams")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 3: CIRCUITS TRANSFORMATION (ROBUST) ---
from pyspark.sql.functions import col, current_timestamp

# --- CONFIGURATION ---
INPUT_PATH_ROOT = "Files/bronze/"
TABLE_NAME_CIRCUITS = "silver_circuits"

# --- 1. READ ---
print(f"Reading Circuits recursively...")
df_circuits_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "circuits.json") \
    .json(INPUT_PATH_ROOT)

# --- 2. TRANSFORMATION ---
print("Transforming Circuits data...")

# Filtramos los test de pretemporada
df_filtered = df_circuits_raw.filter(~col("meeting_name").contains("Testing"))

df_circuits_silver = df_filtered.select(
    col("meeting_key").cast("long"),
    col("circuit_key").cast("long"),       # <--- AÑADIDO: Clave fuerte para joins
    col("meeting_name").alias("gp_name"),
    col("location").alias("city"),
    col("country_name").alias("country"),
    col("country_code").alias("gp_code"),  # <--- AÑADIDO: El código de 3 letras (USA, ESP)
    col("circuit_short_name").alias("circuit"),
    col("date_start").cast("timestamp"),
    col("year").cast("int"),
    current_timestamp().alias("ingestion_date")
)

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME_CIRCUITS}...")

df_circuits_silver.orderBy("date_start") \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(TABLE_NAME_CIRCUITS)

print("✅ Circuits table created successfully with NEW columns!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 4: DRIVERS DIMENSION (FIXED) ---

# --- CONFIGURATION ---
INPUT_PATH = "Files/bronze/" 
TABLE_NAME_DRIVERS = "silver_drivers"
DEFAULT_PHOTO_URL = "https://github.com/NanoVelez/f1-fabric-proyect/blob/main/assets/generic_man_silhouette.png?raw=true"

# --- 1. READ ---
print(f"Reading ALL Drivers catalogs...")

df_drivers_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "drivers.json") \
    .json(INPUT_PATH)

# --- 2. TRANSFORMATION ---
print("Creating drivers history list with COLORS...")

from pyspark.sql.functions import col, when, lit, current_timestamp, concat, input_file_name, regexp_extract

df_drivers_with_year = df_drivers_raw.withColumn("year", regexp_extract(input_file_name(), r"bronze/(\d{4})/", 1).cast("int"))

df_drivers_silver = df_drivers_with_year.select(
    col("driver_number").cast("int"),
    col("full_name"),
    col("name_acronym"),
    col("team_name"),       
    col("headshot_url"),    
    col("country_code"),
    
    when(col("team_colour").isNull() | (col("team_colour") == ""), lit("#999999"))
    .otherwise(concat(lit("#"), col("team_colour"))) 
    .alias("team_colour"),
    
    col("year") 
).withColumn("headshot_url", 
    when((col("headshot_url").isNull()) | (col("headshot_url") == ""), lit(DEFAULT_PHOTO_URL))
    .otherwise(col("headshot_url"))
).withColumn("ingestion_date", current_timestamp()
).dropDuplicates(["driver_number", "year"])

# --- 3. WRITE ---
print(f"Saving Delta table: {TABLE_NAME_DRIVERS}...")

df_drivers_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME_DRIVERS)

print(f"Drivers Dimension created successfully! Rows: {df_drivers_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
