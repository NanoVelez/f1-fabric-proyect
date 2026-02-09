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

# --- CELL: DIMENSIONS (CORRECCIÓN FINAL: NOMBRES DE COLUMNA REALES) ---
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, lit, max as _max, row_number, desc, upper, substring, count, when, size, collect_set

print("🏗️ Generando Dimensiones (Adaptado a tu esquema: gp_name, city, circuit)...")

# ==============================================================================
# 1. DIM DRIVER (Igual que antes)
# ==============================================================================
w_last = Window.partitionBy("driver_number", "year").orderBy(col("meeting_key").desc())

df_rank = spark.table("silver_drivers_standings") \
    .withColumn("rn", row_number().over(w_last)) \
    .filter("rn == 1") \
    .select("driver_number", "year", col("position").cast("int").alias("Season_Rank_Sort"))

spark.table("silver_drivers").alias("d") \
    .join(df_rank.alias("r"), ["driver_number", "year"], "left") \
    .withColumn("Driver_ID", concat(col("driver_number"), lit("-"), col("year"))) \
    .select(
        "Driver_ID",
        col("driver_number").alias("Number"),
        col("year").alias("Year"),
        col("full_name").alias("Driver"),
        col("headshot_url").alias("Driver_Photo"),
        col("country_code").alias("Driver_Country"),
        "Season_Rank_Sort"
    ).dropDuplicates(["Driver_ID"]) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_driver")

# ==============================================================================
# 2. DIM TEAM (Igual que antes)
# ==============================================================================
df_teams = spark.table("silver_teams_standings")
df_drivers = spark.table("silver_drivers")

df_team_joined = df_teams.alias("t") \
    .join(df_drivers.alias("d"), 
          (col("t.team_name") == col("d.team_name")) & 
          (col("t.year") == col("d.year")), 
          "left") \
    .withColumn("Team_ID", concat(col("t.team_name"), lit("-"), col("t.year")))

w_team = Window.partitionBy("Team_ID").orderBy(col("t.year").desc())

df_team_joined.withColumn("rn", row_number().over(w_team)) \
    .filter(col("rn") == 1) \
    .select(
        col("Team_ID"),
        col("t.team_name").alias("Team"),
        col("t.year").alias("Year"),
        col("t.team_logo_url").alias("Team_Logo"),
        col("d.team_colour").alias("Hex_Color")
    ) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_team")

# --- CELL: DIM CIRCUIT (SOLUCIÓN FINAL: DEDUPLICACIÓN DE CLAVES) ---
df_circuits = spark.table("silver_circuits")

# 1. Generar códigos visuales (Misma lógica inteligente de antes)
df_pre = df_circuits.withColumn("Default_Code", upper(substring(col("gp_name"), 1, 3)))
w_conflict = Window.partitionBy("Default_Code")
df_calculated = df_pre.withColumn("Distinct_GPs", size(collect_set("gp_name").over(w_conflict)))

# 2. Crear la Dimensión basada en MEETING_KEY (Evento) y no Circuit_Key (Lugar)
df_gold_race = df_calculated \
    .withColumn("GP_Display", 
        when(col("Distinct_GPs") > 1, 
             upper(substring(col("circuit"), 1, 3))) 
        .otherwise(col("Default_Code"))
    ) \
    .select(
        col("meeting_key"),                      # <--- CLAVE PRINCIPAL (Única por carrera y año)
        col("year").alias("Year"),               # Importante para filtrar
        col("gp_name").alias("Grand_Prix"),      
        col("circuit").alias("Circuit_Name"),    
        col("GP_Display"),                       
        col("date_start").alias("Date"),         # Fecha REAL de ese año específico   
    ).distinct()

# Guardamos como gold_dim_race (Dimensión de Carrera)
df_gold_race.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_race")
# ==============================================================================
# 4. DIM YEAR
# ==============================================================================
spark.table("silver_circuits") \
    .select(col("year").alias("Year")) \
    .distinct() \
    .orderBy(col("Year").desc()) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_year")

print("✅ Todo listo. Dimensiones generadas usando columnas: gp_name, circuit, city.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: FACT DRIVER (FIX FINAL - INDENTATION FIXED) ---
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, concat, lit, max as _max, when

print("🏗️ Generando Facts corrigiendo el salto de equipo (Bearman)...")

# 1. CARGA Y LIMPIEZA
df_standings_raw = spark.table("silver_drivers_standings")

# Limpiamos duplicados por meeting
df_standings_clean = df_standings_raw \
    .groupBy("driver_number", "year", "meeting_key") \
    .agg(_max("points").alias("points"), _max("position").alias("position")) \
    .select("driver_number", "year", "meeting_key", "points", "position")

df_circuits = spark.table("silver_circuits")
df_drivers = spark.table("silver_drivers") 

# 2. JOINS
df_joined = df_standings_clean.alias("f") \
    .join(df_circuits.alias("c"), col("f.meeting_key") == col("c.meeting_key")) \
    .join(df_drivers.alias("d"), 
          (col("f.driver_number") == col("d.driver_number")) & 
          (col("f.year") == col("d.year")), 
          "left")

# 3. CÁLCULO DELTA (FIX 🛠️)
# Agrupamos por NOMBRE COMPLETO para conectar Ferrari con Haas
w_diff = Window.partitionBy("d.full_name", "f.year").orderBy("c.date_start")

df_fact_driver = df_joined \
    .withColumn("prev_points", lag("f.points", 1, 0.0).over(w_diff)) \
    .withColumn("Race_Points", (col("f.points") - col("prev_points")).cast("float")) \
    .withColumn("Driver_Key", (col("f.year") * 10000 + col("f.driver_number")).cast("long")) \
    .withColumn("Team_ID", concat(col("d.team_name"), lit("-"), col("f.year"))) \
    .select(
        col("Driver_Key"),
        col("Team_ID"),
        col("c.meeting_key"),
        col("f.year").alias("Year"),
        col("f.driver_number"),
        
        # Evitar negativos
        when(col("Race_Points") < 0, 0).otherwise(col("Race_Points")).alias("Race_Points"),
        
        col("f.points").alias("Season_Points"),
        col("f.position").alias("World_Position")
    )

# Guardar
df_fact_driver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_driver_results")

print("✅ Fact Table arreglada sin errores de indentación.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: FACT TEAM RESULTS (USANDO RANKING OFICIAL) ---
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, concat, lit

print("🏗️ Generando Fact Teams usando 'position' oficial de la API...")

# 1. Cargar tablas
#    silver_teams_standings ya tiene el acumulado y la posición oficial
df_t_standings = spark.table("silver_teams_standings") 
df_circuits = spark.table("silver_circuits")

# 2. Join con Circuitos para tener fechas y keys
df_joined_t = df_t_standings.alias("f") \
    .join(df_circuits.alias("c"), col("f.meeting_key") == col("c.meeting_key"))

# 3. Ventana SOLO para calcular los puntos de la carrera (Race_Points)
#    Restamos los puntos de esta carrera menos los de la anterior para saber cuánto ganaron hoy.
w_diff_t = Window.partitionBy("f.team_name", "f.year").orderBy("c.date_start")

df_fact_team = df_joined_t \
    .withColumn("Team_ID", concat(col("f.team_name"), lit("-"), col("f.year"))) \
    .withColumn("prev_points", lag("f.points", 1, 0).over(w_diff_t)) \
    .withColumn("Race_Points", (col("f.points") - col("prev_points")).cast("float")) \
    .select(
        col("Team_ID"),
        col("c.meeting_key"),
        col("f.year").alias("Year"),
        
        # --- LOS DATOS CLAVE ---
        col("Race_Points"),                          # Puntos del día (calculado)
        col("f.points").alias("Season_Points"),      # Puntos acumulados (API)
        col("f.position").alias("World_Position")    # <--- RANKING OFICIAL (API)
    )

df_fact_team.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_team_results")

print("✅ Fact Teams actualizada. Ranking de Constructores listo.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: GOLD UI TIMELINE (CON MEETING KEY PARA RELACIÓN LIMPIA) ---
from pyspark.sql.functions import col

print("🏗️ Generando Tabla Auxiliar UI con MEETING KEY...")

# 1. Leemos silver_circuits (o gold_dim_race / gold_fact_driver_results)
# Lo ideal es leer de la misma fuente que usaste para gold_dim_race para asegurar consistencia
df_source = spark.table("silver_circuits") 

# 2. Seleccionamos Fecha, GP y la LLAVE
df_timeline = df_source \
    .select(
        col("date_start").alias("Date"), 
        col("gp_name"),
        col("meeting_key").cast("int").alias("meeting_key"), # <--- ¡LA LLAVE MAESTRA!
        col("year").cast("int").alias("Year") # Opcional, pero útil para validar visualmente
    ) \
    .distinct() \
    .orderBy("Date")

# 3. Guardamos
df_timeline.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_ui_play_timeline")

print("✅ Tabla 'gold_ui_play_timeline' actualizada con meeting_key.")
display(df_timeline)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: FIX DRIVER GROUPING (Solución para Matriz) ---
from pyspark.sql.functions import col, trim

print("🏗️ Añadiendo columna de agrupación a 'gold_dim_driver'...")

# 1. Leemos la tabla original
df_driver = spark.table("gold_dim_driver")

# 2. Creamos la columna nueva (Copia exacta del nombre para agrupar)
# Usamos trim() por seguridad para quitar espacios invisibles
df_driver = df_driver.withColumn("Driver_Grouping_Name", trim(col("Driver")))

# 3. Guardamos (Sobrescribiendo la tabla Gold)
df_driver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_driver")

print("✅ Tabla 'gold_dim_driver' actualizada.")
print("   - Ahora tienes 'Driver_Grouping_Name' disponible.")
print("   - Ve a Power BI y dale a 'Actualizar' (Refresh).")
display(df_driver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- CELL: GOLD DIM DRIVER (FIX FINAL - ÚLTIMO RANKING) ---
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

print("🏗️ Generando Dim Driver con el RANKING FINAL (Última carrera)...")

# 1. Preparar datos: Unimos Standings con Drivers para tener Nombres y Fechas (meeting_key)
df_drivers = spark.table("silver_drivers")
df_standings = spark.table("silver_drivers_standings")

# Hacemos join para tener (Nombre, Año, Meeting_Key, Posición)
df_joined = df_drivers.alias("d") \
    .join(df_standings.alias("s"), 
          (col("d.driver_number") == col("s.driver_number")) & 
          (col("d.year") == col("s.year")), 
          "inner") \
    .select(
        col("d.full_name"),
        col("d.year"),
        col("s.meeting_key"),
        col("s.position").cast("int").alias("rank_in_race")
    )

# 2. ⭐️ LA LÓGICA CLAVE: Buscar el ranking de la ÚLTIMA carrera ⭐️
#    Para cada Persona (Nombre) y Año, ordenamos por carrera (meeting_key) DESCENDENTE.
#    El primero que salga (rn=1) es su estado final de temporada.
w_last_status = Window.partitionBy("full_name", "year").orderBy(col("meeting_key").desc())

df_final_ranks = df_joined \
    .withColumn("rn", row_number().over(w_last_status)) \
    .filter("rn == 1") \
    .select(
        col("full_name"), 
        col("year"), 
        col("rank_in_race").alias("Final_Season_Rank") # Este será el 18 para Bearman
    )

# 3. Pegar este ranking "Correcto" a la tabla final de Drivers
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
        col("r.Final_Season_Rank").alias("Season_Rank_Sort") # <--- AHORA SÍ: El rank final real
    ) \
    .dropDuplicates(["Driver_Key"])

# 4. Guardar
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_driver")

print("✅ Dimensión corregida.")
print("   - Oliver Bearman ahora tendrá Season_Rank_Sort = 18 en ambas filas.")
display(df_final.filter("Number = 38 OR Number = 50"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
