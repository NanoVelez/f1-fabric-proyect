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

# --- CELL 0: GOLD GLOBAL SETUP ---

# 1. Analytical Imports (Essential for Gold Layer)
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, when, concat, substring, upper, trim,
    max as _max, min as _min, count, size, collect_set,
    row_number, desc, lag
)

# 2. Spark Configuration
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

print("Gold Environment Ready: Analytical libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 1: GOLD DIM DRIVER ---
TABLE_NAME = "gold_dim_driver"

print(f"Building {TABLE_NAME}...")

# 1. Sources
df_drivers = spark.table("silver_drivers")
df_standings = spark.table("silver_drivers_standings")
df_circuits = spark.table("silver_circuits") 

# 2. Preparation
df_standings_with_date = df_standings.alias("s") \
    .join(df_circuits.alias("c"), col("s.meeting_key") == col("c.meeting_key"), "inner") \
    .select(
        col("s.driver_number"),
        col("s.year"),
        col("s.meeting_key"),
        col("s.position"),
        col("c.date_start") 
    )

# 3. Join to get Driver + Results + Date
df_joined = df_drivers.alias("d") \
    .join(df_standings_with_date.alias("s"), 
          (col("d.driver_number") == col("s.driver_number")) & 
          (col("d.year") == col("s.year")), 
          "inner") \
    .select(
        col("d.full_name"),
        col("d.year"),
        col("s.meeting_key"),
        col("s.date_start"),
        col("s.position").cast("int").alias("rank_in_race")
    )

# 4. KEY LOGIC
w_last_status = Window.partitionBy("full_name", "year").orderBy(col("date_start").desc())

df_final_ranks = df_joined \
    .withColumn("rn", row_number().over(w_last_status)) \
    .filter("rn == 1") \
    .select(
        col("full_name"), 
        col("year"), 
        col("rank_in_race").alias("Final_Season_Rank") 
    )

# 5. Final Assembly
df_final = spark.table("silver_drivers").alias("d") \
    .join(df_final_ranks.alias("r"), 
          (col("d.full_name") == col("r.full_name")) & 
          (col("d.year") == col("r.year")), 
          "left") \
    .withColumn("Driver_Key", (col("d.year") * 10000 + col("d.driver_number")).cast("long")) \
    .select(
        "Driver_Key",
        col("d.driver_number").alias("Number"),
        col("d.year").alias("Year"),
        col("d.full_name").alias("Driver"),
        col("d.headshot_url").alias("Driver_Photo"),
        col("d.country_code").alias("Driver_Country"),
        col("r.Final_Season_Rank").alias("Season_Rank_Sort") 
    ) \
    .dropDuplicates(["Driver_Key"])

# 6. Write
df_final.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 2: GOLD DIM TEAM ---

# Config
TABLE_NAME = "gold_dim_team"

print(f"Building {TABLE_NAME}...")

# 1. Sources
df_teams_silver = spark.table("silver_teams") 
df_drivers = spark.table("silver_drivers")

# 2. Get Unique Color per Team/Year
df_colors = df_drivers \
    .where(col("team_name").isNotNull()) \
    .groupBy("team_name", "year") \
    .agg(_max("team_colour").alias("Hex_Color")) 

# 3. Join and Final Selection
df_gold_team = df_teams_silver.alias("t") \
    .join(df_colors.alias("c"), 
          (col("t.team_name") == col("c.team_name")) & 
          (col("t.year") == col("c.year")), 
          "left") \
    .select(
        concat(trim(col("t.team_name")), lit("-"), col("t.year")).alias("Team_Key"),
        trim(col("t.team_name")).alias("Team"),
        col("t.year").alias("Year"),
        col("t.team_logo_url").alias("Team_Logo"),
        when(col("c.Hex_Color").isNull(), "#999999").otherwise(col("c.Hex_Color")).alias("Hex_Color")
    ) \
    .dropDuplicates(["Team_Key"])

# 4. Write
df_gold_team.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 3: GOLD DIM RACE ---

# Config
TABLE_NAME = "gold_dim_race"

print(f"Building {TABLE_NAME}...")

# 1. Source
df_circuits = spark.table("silver_circuits")

# 2. Generate Visual Codes logic
df_pre = df_circuits.withColumn("Default_Code", upper(substring(col("gp_name"), 1, 3)))
w_conflict = Window.partitionBy("Default_Code")
df_calculated = df_pre.withColumn("Distinct_GPs", size(collect_set("gp_name").over(w_conflict)))

# 3. Create Dimension
df_gold_race = df_calculated \
    .withColumn("GP_Display", 
        when(col("Distinct_GPs") > 1, 
             upper(substring(col("circuit"), 1, 3)))
        .otherwise(col("Default_Code"))
    ) \
    .select(
        col("meeting_key").alias("Race_Key"),   
        col("year").alias("Year"),               
        col("gp_name").alias("Grand_Prix"),      
        col("circuit").alias("Circuit_Name"),    
        col("GP_Display"),                       
        col("date_start").alias("Date"),         
    ).distinct()

# 4. Write
df_gold_race.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 4: GOLD FACT DRIVER RESULTS ---

# Config
TABLE_NAME = "gold_fact_driver_results"

print(f"Building {TABLE_NAME}...")

# 1. Sources
df_standings_raw = spark.table("silver_drivers_standings")
df_circuits = spark.table("silver_circuits")
df_drivers = spark.table("silver_drivers") 

# 2. Pre-processing
df_standings_clean = df_standings_raw \
    .groupBy("driver_number", "year", "meeting_key") \
    .agg(_max("points").alias("points"), _max("position").alias("position")) \
    .select("driver_number", "year", "meeting_key", "points", "position")

# 3. Joins
df_joined = df_standings_clean.alias("f") \
    .join(df_circuits.alias("c"), col("f.meeting_key") == col("c.meeting_key")) \
    .join(df_drivers.alias("d"), 
          (col("f.driver_number") == col("d.driver_number")) & 
          (col("f.year") == col("d.year")), 
          "left")

# 4. DELTA CALCULATION LOGIC
w_diff = Window.partitionBy("d.full_name", "f.year").orderBy("c.date_start")

df_fact_driver = df_joined \
    .withColumn("prev_points", lag("f.points", 1, 0.0).over(w_diff)) \
    .withColumn("Race_Points", (col("f.points") - col("prev_points")).cast("float")) \
    .withColumn("Driver_Key", (col("f.year") * 10000 + col("f.driver_number")).cast("long")) \
    .withColumn("Team_Key", concat(col("d.team_name"), lit("-"), col("f.year"))) \
    .select(
        col("Driver_Key"),
        col("Team_Key"),
        col("c.meeting_key").alias("Race_Key"),
        when(col("Race_Points") < 0, 0).otherwise(col("Race_Points")).alias("Race_Points"),
        col("f.points").alias("Season_Points"),
        col("f.position").alias("World_Position")
    )

# 5. Write
df_fact_driver.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 5: GOLD FACT TEAM RESULTS ---

# Config
TABLE_NAME = "gold_fact_team_results"

print(f"Building {TABLE_NAME}...")

# 1. Sources
df_t_standings = spark.table("silver_teams_standings") 
df_circuits = spark.table("silver_circuits")

# 2. Join with Circuits for Dates
df_joined_t = df_t_standings.alias("f") \
    .join(df_circuits.alias("c"), col("f.meeting_key") == col("c.meeting_key"))

# 3. WINDOW DEFINITIONS
w_fix_drop = Window.partitionBy("f.team_name", "f.year") \
                   .orderBy("c.date_start") \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)

#    B. Diff Window
w_diff_t = Window.partitionBy("f.team_name", "f.year").orderBy("c.date_start")

# 4. TRANSFORMATION
df_fact_team = df_joined_t \
    .withColumn("Team_Key", concat(col("f.team_name"), lit("-"), col("f.year"))) \
    .withColumn("Season_Points_Fixed", _max("f.points").over(w_fix_drop)) \
    .withColumn("prev_points", lag("Season_Points_Fixed", 1, 0).over(w_diff_t)) \
    .withColumn("Race_Points", (col("Season_Points_Fixed") - col("prev_points")).cast("float")) \
    .select(
        col("Team_Key"),
        col("c.meeting_key").alias("Race_Key"),
        when(col("Race_Points") < 0, 0).otherwise(col("Race_Points")).alias("Race_Points"),
        col("Season_Points_Fixed").alias("Season_Points"),
        col("f.position").alias("World_Position")
    )

# 5. Write
df_fact_team.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL 6: GOLD UI PLAY TIMELINE ---

# Config
TABLE_NAME = "gold_ui_play_timeline"

print(f"Building {TABLE_NAME}...")

# 1. Source
df_source = spark.table("silver_circuits") 

# 2. Selection
df_timeline = df_source \
    .select(
        col("date_start").alias("Date"), 
        col("meeting_key").cast("int").alias("Race_Key"),
    ) \
    .distinct() \
    .orderBy("Date")

# 3. Write
df_timeline.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"{TABLE_NAME} CREATED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
