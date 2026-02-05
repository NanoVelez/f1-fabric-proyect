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

from pyspark.sql.functions import col

# --- CONFIGURATION ---
TABLE_NAME_GOLD = "gold_driver_standings"

# --- 1. LOAD SILVER TABLES ---
print("Loading Silver tables...")

df_facts = spark.table("silver_drivers_standings")

df_dim_drivers = spark.table("silver_drivers")
df_dim_circuits = spark.table("silver_circuits")

# --- 2. THE JOIN ---
print("Joining Data (Facts + Dimensions)...")

df_step1 = df_facts.join(df_dim_drivers, on="driver_number", how="left")

df_final_gold = df_step1.join(df_dim_circuits, on="meeting_key", how="left")

# --- 3. SELECTION & CLEANING ---
print("Selecting final columns for Power BI...")

df_gold_clean = df_final_gold.select(
    col("full_name").alias("driver_name"),
    col("name_acronym"),
    col("driver_number"),
    col("team_name"),
    col("headshot_url"),
    
    col("position"),
    col("points"),
    
    col("gp_name"),
    col("circuit"),
    col("country"),
    col("date_start"),
    col("season_year")
)

# --- 4. WRITE ---
print(f"Saving Gold Table: {TABLE_NAME_GOLD}...")

df_gold_clean.orderBy("date_start", "position") \
    .write.mode("overwrite").format("delta").saveAsTable(TABLE_NAME_GOLD)

print("GOLD LAYER COMPLETE! Ready for Power BI.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
