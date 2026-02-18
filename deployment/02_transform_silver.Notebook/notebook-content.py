# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

YEAR_PARAM = "2023"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 0: GLOBAL SETUP & IMPORTS ---
from pyspark.sql.functions import (
    col, lit, when, concat, split, element_at, 
    input_file_name, regexp_extract, regexp_replace, 
    current_timestamp, max as _max
)
from pyspark.sql.window import Window 

# 1. Spark Configuration
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 2. DYNAMIC PATHS
PATH_BRONZE = f"Files/bronze/{YEAR_PARAM}/" 
PATH_SILVER = "Files/silver/" 

print(f"CONFIG READY. Processing Year: {YEAR_PARAM}")
print(f"Reading from: {PATH_BRONZE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 1: SILVER DRIVERS STANDINGS ---

# --- CONFIGURATION ---
TABLE_NAME = "silver_drivers_standings"

print(f"Building {TABLE_NAME}...")

# --- 1. READ ---
df_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "drivers_standings.json") \
    .json(PATH_BRONZE)

# --- 2. TRANSFORMATION ---
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
df_silver.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 2: SILVER TEAMS STANDINGS ---

# --- CONFIGURATION ---
TABLE_NAME = "silver_teams_standings"

print(f"Building {TABLE_NAME}...")

# 1. READ & TRANSFORM
df_silver = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "teams_standings.json") \
    .json(PATH_BRONZE) \
    .select(
        col("meeting_key").cast("long"),
        col("team_name"), 
        col("position_current").cast("int").alias("position"),
        col("points_current").cast("float").alias("points"),
        
        # Metadata
        element_at(split(input_file_name(), "/"), -3).cast("int").alias("year"),
        current_timestamp().alias("ingestion_date")
    ).na.drop(subset=["team_name", "position"])

# 2. WRITE
df_silver.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 3: SILVER CIRCUITS ---

# --- CONFIGURATION ---
TABLE_NAME = "silver_circuits"

print(f"Building {TABLE_NAME}...")

# --- 1. READ ---
df_circuits_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "circuits.json") \
    .json(PATH_BRONZE)

# --- 2. TRANSFORMATION ---
df_filtered = df_circuits_raw.filter(~col("meeting_name").contains("Testing"))

df_circuits_silver = df_filtered.select(
    col("meeting_key").cast("long"),
    col("circuit_key").cast("long"),       
    col("meeting_name").alias("gp_name"),
    col("location").alias("city"),
    col("country_name").alias("country"),
    col("country_code").alias("gp_code"),  
    col("circuit_short_name").alias("circuit"),
    col("date_start").cast("timestamp"),
    col("year").cast("int"),
    current_timestamp().alias("ingestion_date")
)

# --- 3. WRITE ---
df_circuits_silver.orderBy("date_start") \
    .write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 4: SILVER DRIVERS ---

# CONFIG
TABLE_NAME = "silver_drivers"
DEFAULT_PHOTO = "https://github.com/NanoVelez/f1-fabric-proyect/blob/main/assets/generic_man_silhouette.png?raw=true"

print(f"Building {TABLE_NAME}...")

# 1. READ & TRANSFORM
df_drivers_silver = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "drivers.json") \
    .json(PATH_BRONZE) \
    .withColumn("year", regexp_extract(input_file_name(), r"bronze/(\d{4})/", 1).cast("int")) \
    .select(
        col("driver_number").cast("int"),
        col("full_name"),
        col("name_acronym"),
        col("team_name"),       
        col("country_code"),
        col("year"),
        
        when((col("headshot_url").isNull()) | (col("headshot_url") == ""), lit(DEFAULT_PHOTO))
        .otherwise(col("headshot_url")).alias("headshot_url"),

        when((col("team_colour").isNull()) | (col("team_colour") == ""), lit("#999999"))
        .otherwise(concat(lit("#"), col("team_colour"))).alias("team_colour"),
        
        current_timestamp().alias("ingestion_date")
    ).dropDuplicates(["driver_number", "year"])

# 2. WRITE
df_drivers_silver.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 5: SILVER TEAMS ---

# CONFIG
TABLE_SOURCE = "silver_teams_standings" 
TABLE_NAME = "silver_teams"
GITHUB_ASSETS_URL = "https://raw.githubusercontent.com/NanoVelez/f1-fabric-proyect/main/assets/teams/"

print(f"Building {TABLE_NAME}...")

# 1. READ & DEDUPLICATE
df_teams_unique = spark.table(TABLE_SOURCE) \
    .groupBy("team_name", "year") \
    .agg(_max("ingestion_date").alias("last_updated"))

# 2. ENRICH
df_teams_silver = df_teams_unique.withColumn(
    "team_logo_url", 
    concat(
        lit(GITHUB_ASSETS_URL), 
        regexp_replace(col("team_name"), " ", "%20"), 
        lit(".png")
    )
).select(
    col("team_name"),
    col("year"),
    col("team_logo_url"),
    col("last_updated")
)

# 3. WRITE
df_teams_silver.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .partitionBy("year") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
