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

from pyspark.sql.functions import col, row_number, desc, lag
from pyspark.sql.window import Window

# --- CONFIGURATION ---
TABLE_NAME_GOLD_HISTORY = "gold_driver_standings"       
TABLE_NAME_GOLD_LATEST  = "gold_driver_ranking_latest"  

## --- 1. LOAD SILVER TABLES ---
print("Loading Silver tables...")

df_facts = spark.table("silver_drivers_standings")
df_dim_drivers = spark.table("silver_drivers")
df_dim_circuits = spark.table("silver_circuits")
df_dim_teams = spark.table("silver_teams_standings") 

# --- 2. THE JOINS ---
print("Joining Data with Team Logos...")

# Join 1: Pilots
# CORRECCIÓN: Añadido .drop(df_dim_drivers["driver_number"]) para evitar duplicados
df_step1 = df_facts.join(
    df_dim_drivers, 
    (df_facts["driver_number"] == df_dim_drivers["driver_number"]) & 
    (df_facts["season_year"] == df_dim_drivers["season"]), 
    how="left"
).drop(df_dim_drivers["season"]).drop(df_dim_drivers["driver_number"]) 

# Join 2: Circuits
df_step2 = df_step1.join(df_dim_circuits, on="meeting_key", how="left")

# Join 3: Teams 
df_final_gold = df_step2.join(
    df_dim_teams.select("team_name", "season_year", "team_logo_url").distinct(),
    on=["team_name", "season_year"],
    how="left"
)

# --- 2.5 CALCULO DIFERENCIAL --- 
print("Calculating Points per Race...")

# Ahora ya no dará error de ambigüedad porque solo queda un 'driver_number'
w_calc = Window.partitionBy("driver_number", "season_year").orderBy("date_start")

# Usamos col("points") directamente
df_calculated = df_final_gold.withColumn("prev_points", lag(col("points"), 1, 0).over(w_calc)) \
                             .withColumn("Points_Per_Race", col("points") - col("prev_points"))

# --- 3. SELECTION & CLEANING ---
df_gold_clean = df_calculated.select(
    col("full_name").alias("Driver"),
    col("team_name").alias("Team"),
    col("team_logo_url").alias("Team_Logo"),
    col("team_colour").alias("Hex_Color"), 
    col("headshot_url").alias("Driver_Photo"),
    col("position").alias("Position"),
    
    # Usamos la columna calculada
    col("Points_Per_Race").cast("float").alias("Points"), 
    
    col("gp_name").alias("Grand_Prix"),
    col("gp_code").alias("GP_Code"),
    col("circuit").alias("Circuit"),
    col("country").alias("Contry"),
    col("date_start").alias("Date"),
    col("season_year").alias("Year")
)

# --- 4. WRITE TABLE 1: HISTORY ---
print(f"Saving Full History Table: {TABLE_NAME_GOLD_HISTORY}...")

df_gold_clean.orderBy("Date", "Position") \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .partitionBy("Year") \
    .saveAsTable(TABLE_NAME_GOLD_HISTORY)


# --- 5. WRITE TABLE 2: SNAPSHOT ---
print(f"Generating Latest Snapshot for Power BI Cards...")

windowSpec = Window.partitionBy("Year", "Driver").orderBy(desc("Date"))

df_latest = df_gold_clean.withColumn("row_num", row_number().over(windowSpec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"Saving Snapshot Table: {TABLE_NAME_GOLD_LATEST}...")

df_latest.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(TABLE_NAME_GOLD_LATEST)

print("GOLD LAYER COMPLETE!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: GOLD CONSTRUCTORS (FIXED DUPLICATES) ---
from pyspark.sql.functions import col, row_number, desc, max as spark_max
from pyspark.sql.window import Window

# Nombres
TABLE_CONSTR_HISTORY = "gold_constructor_standings"   
TABLE_CONSTR_LATEST  = "gold_constructor_leaderboard" 

# 1. CARGAMOS TABLAS
print("Loading tables...")
df_teams_facts = spark.table("silver_teams_standings")
df_circuits = spark.table("silver_circuits")
df_drivers = spark.table("silver_drivers") 

# 2. PREPARAMOS EL COLOR (EL FIX ESTÁ AQUÍ 🛠️)
# En lugar de distinct(), usamos groupBy para asegurar 1 sola fila por equipo/año
df_colors_unique = df_drivers.groupBy(
    col("team_name").alias("driver_team_name"), 
    col("season").alias("driver_season")
).agg(
    spark_max("team_colour").alias("Hex_Color") # Cogemos UN solo color (el maximo)
)

# 3. JOINS
print("Joining Constructors Data...")

# Join 1: Teams + Circuits
df_step1 = df_teams_facts.join(df_circuits, on="meeting_key", how="left")

# Join 2: + Colors (Ahora es seguro porque df_colors_unique es único)
df_final = df_step1.join(
    df_colors_unique, 
    (df_step1["team_name"] == df_colors_unique["driver_team_name"]) & 
    (df_step1["season_year"] == df_colors_unique["driver_season"]),
    how="left"
)

# 4. SELECCIÓN
print("Selecting final columns...")
df_gold_constr = df_final.select(
    df_teams_facts["team_name"].alias("Team"), 
    df_teams_facts["team_logo_url"].alias("Team_Logo"),
    col("Hex_Color"), # Color único garantizado
    df_teams_facts["position"].alias("Position"),
    df_teams_facts["points"].alias("Points"),
    col("gp_name").alias("Grand_Prix"),
    col("date_start").alias("Date"),
    df_teams_facts["season_year"].alias("Year")
)

# 5. GUARDAR HISTÓRICO
print(f"Saving Constructors History...")
df_gold_constr.orderBy("Date", "Position").write \
    .mode("overwrite").option("overwriteSchema", "true").format("delta") \
    .partitionBy("Year").saveAsTable(TABLE_CONSTR_HISTORY)

# 6. GUARDAR LEADERBOARD
print(f"Saving Constructors Leaderboard...")
windowSpec = Window.partitionBy("Year", "Team").orderBy(desc("Date"))
df_latest_constr = df_gold_constr.withColumn("row_num", row_number().over(windowSpec)) \
    .filter(col("row_num") == 1).drop("row_num")

df_latest_constr.write \
    .mode("overwrite").option("overwriteSchema", "true").format("delta") \
    .saveAsTable(TABLE_CONSTR_LATEST)

print("✅ FIXED: Duplicados eliminados. Los puntos ahora serán correctos.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- TABLA MAESTRA DE AÑOS (DIM_YEAR) ---
# Extraemos los años únicos de los standings de constructores
df_years = spark.table("gold_constructor_standings") \
    .select("Year") \
    .distinct() \
    .orderBy("Year")

# La guardamos como una tabla física en el Lakehouse
df_years.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_dim_year")

print("✅ Tabla maestra 'gold_dim_year' creada en el Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: TABLA MAESTRA DE EQUIPOS (DIM_TEAM) ---
from pyspark.sql.functions import col

print("Generando tabla maestra de equipos (Dim_Team)...")

# 1. Leemos los datos de la tabla de constructores (que ya tiene los logos limpios)
df_gold_source = spark.table("gold_constructor_standings")

# 2. Seleccionamos solo los equipos únicos y sus logos
df_dim_team = df_gold_source.select(
    col("Team"),
    col("Team_Logo")
).distinct().orderBy("Team")

# 3. Guardamos la tabla física en el Lakehouse
TABLE_NAME_DIM = "gold_dim_team"

df_dim_team.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(TABLE_NAME_DIM)

print(f"✅ Tabla '{TABLE_NAME_DIM}' creada exitosamente en el Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: TABLA MAESTRA DE PILOTOS (DIM_DRIVER) ---
from pyspark.sql.functions import col

print("Generando tabla maestra de pilotos (Dim_Driver)...")

# 1. Leemos la tabla de hechos que acabas de generar (History)
#    Así nos aseguramos de que no falte ningún piloto que tenga puntos.
df_gold_source = spark.table("gold_driver_standings")

# 2. Seleccionamos solo las columnas fijas del piloto
#    NOTA: No incluimos 'Team' ni 'Hex_Color' aquí para evitar duplicados 
#    si un piloto cambió de equipo (ej. Hamilton en Mercedes/Ferrari).
df_dim_driver = df_gold_source.select(
    col("Driver"),
).distinct().orderBy("Driver")

# 3. Guardamos la tabla física en el Lakehouse
TABLE_NAME_DIM = "gold_dim_driver"

df_dim_driver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(TABLE_NAME_DIM)

print(f"✅ Tabla '{TABLE_NAME_DIM}' creada exitosamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: DIMENSIONS (Driver, Team, Circuit) ---
from pyspark.sql.functions import col

# 1. DIM DRIVER (Solo quién es y su foto)
spark.table("silver_drivers") \
    .select(
        col("full_name").alias("Driver"),
        col("headshot_url").alias("Driver_Photo"),
        col("country_code").alias("Driver_Country")
    ).distinct() \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_driver")

# 2. DIM TEAM (Solo quién es, logo y color)
spark.table("silver_teams_standings") \
    .join(spark.table("silver_drivers"), "team_name") \
    .select(
        col("team_name").alias("Team"),
        col("team_logo_url").alias("Team_Logo"),
        col("team_colour").alias("Hex_Color")
    ).distinct() \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_team")

# 3. DIM CIRCUIT (Nueva: Para limpiar las Facts)
spark.table("silver_circuits") \
    .select(
        col("gp_code").alias("GP_Code"), # Tu clave de 3 letras
        col("gp_name").alias("Grand_Prix"),
        col("circuit").alias("Circuit_Name"),
        col("country").alias("Country")
    ).distinct() \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_circuit")

print("✅ Dimensiones creadas y limpias.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: FACT DRIVER RESULTS (Optimizada) ---
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, sum as _sum

# Cargamos tablas Silver
df_standings = spark.table("silver_drivers_standings")
df_circuits = spark.table("silver_circuits")
df_drivers = spark.table("silver_drivers")  # La cargo aquí para tenerlo ordenado

# Cálculo de Puntos por Carrera
w = Window.partitionBy("driver_number", "year").orderBy("date_start")

# AQUI ESTA EL CAMBIO (Opción 1 applied)
# Al poner ["meeting_key", "year"], Spark fusiona las columnas year de ambas tablas.
df_fact = df_standings.join(df_circuits, ["meeting_key", "year"], "inner") \
    .withColumn("prev_points", lag("points", 1, 0).over(w)) \
    .withColumn("Race_Points", (col("points") - col("prev_points")).cast("float")) \
    .join(df_drivers, ["driver_number", "year"], "inner") \
    .select(
        col("year").alias("Year"),
        col("full_name").alias("Driver"),
        col("team_name").alias("Team"),  # Asegúrate que team_name esté en drivers o standings
        col("gp_code").alias("GP_Code"),
        col("date_start").alias("Date"),
        col("position").alias("Position"),
        col("Race_Points").alias("Points") 
    )

df_fact.orderBy("Date", "Position") \
    .write.mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("Year") \
    .saveAsTable("gold_fact_driver_results")

print("✅ Tabla de Hechos optimizada creada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: FACT TEAM RESULTS (Constructors) ---
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, sum as _sum

print("Generando Fact Team Results...")

# 1. Cargamos las tablas Silver
df_team_standings = spark.table("silver_teams_standings")
df_circuits = spark.table("silver_circuits")

# 2. Unimos con Circuitos para tener la Fecha y el Código de País
#    (Necesario para ordenar cronológicamente)
df_joined = df_team_standings.join(
    df_circuits.select("meeting_key", "date_start", "gp_code"), 
    on="meeting_key", 
    how="inner"
)

# 3. Cálculo Diferencial (Reverse Engineering) 🧮
#    Restamos los puntos de la carrera anterior para sacar los de "hoy".
w_team_calc = Window.partitionBy("team_name", "year").orderBy("date_start")

df_calculated = df_joined.withColumn("prev_points", lag("points", 1, 0).over(w_team_calc)) \
    .withColumn("Race_Points", (col("points") - col("prev_points")).cast("float"))

# 4. Selección Limpia (Solo claves y hechos)
df_fact_team = df_calculated.select(
    # CLAVES (Para unir con Dims)
    col("year").alias("Year"),
    col("date_start").alias("Date"),
    col("gp_code").alias("GP_Code"),
    col("team_name").alias("Team"), # Clave para unir con gold_dim_team
    
    # HECHOS (Datos numéricos)
    col("position").alias("Position"),
    col("Race_Points").alias("Points")
)

# 5. Guardado
TABLE_NAME_FACT_TEAM = "gold_fact_team_results"

print(f"Guardando {TABLE_NAME_FACT_TEAM}...")

df_fact_team.orderBy("Date", "Position") \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .partitionBy("Year") \
    .saveAsTable(TABLE_NAME_FACT_TEAM)

print("✅ Tabla de Hechos de Equipos creada exitosamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
