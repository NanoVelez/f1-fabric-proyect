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

import requests
import json
from notebookutils import mssparkutils
import time

# --- CONFIGURATION ---
BASE_URL = "https://api.openf1.org/v1"
YEAR = "2023"
PATH_BRONZE = f"Files/bronze/{YEAR}"

# --- 1. CLEANUP ---
print(f"Cleaning landing zone: {PATH_BRONZE}...")
try:
    mssparkutils.fs.rm(PATH_BRONZE, True)
except:
    pass

# --- 2. DATA PREPARATION ---
print("Loading catalog...")

# A. Get all RACE sessions first
races_raw = requests.get(f"{BASE_URL}/sessions?year={YEAR}&session_type=Race").json()
race_lookup = {r['meeting_key']: r['session_key'] for r in races_raw}

# B. Get all MEETINGS (Events)
meetings = requests.get(f"{BASE_URL}/meetings?year={YEAR}").json()
meetings_sorted = sorted(meetings, key=lambda x: x['date_start'])

print(f"Found {len(meetings_sorted)} scheduled events.")
print("Starting ingestion...\n")

# --- 3. MAIN LOOP (Events) ---
round_counter = 1

for meeting in meetings_sorted:
    meeting_key = meeting['meeting_key']
    official_name = meeting['meeting_name']
    
    # FILTER: Skip Testing
    if "Testing" in official_name:
        continue
        
    session_key_race = race_lookup.get(meeting_key)
    if not session_key_race:
        continue

    # --- PROCESSING ---
    clean_name = official_name.replace(" ", "_")
    folder_name = f"{str(round_counter).zfill(2)}_{clean_name}"
    base_path = f"{PATH_BRONZE}/{folder_name}"
    
    print(f"Round {round_counter}: {clean_name}")
    
    try:
        # ==========================================
        # 1. DESCARGAS GENERALES (Con pausas)
        # ==========================================
        
        # Drivers Standings
        url_d = f"{BASE_URL}/championship_drivers?session_key={session_key_race}"
        mssparkutils.fs.put(f"{base_path}/drivers_standings.json", json.dumps(requests.get(url_d).json()), True)
        time.sleep(0.5) # <--- PAUSA OBLIGATORIA

        # Teams Standings
        url_t = f"{BASE_URL}/championship_teams?session_key={session_key_race}"
        mssparkutils.fs.put(f"{base_path}/teams_standings.json", json.dumps(requests.get(url_t).json()), True)
        time.sleep(0.5) # <--- PAUSA OBLIGATORIA

        # Drivers Roster
        url_dr = f"{BASE_URL}/drivers?session_key={session_key_race}"
        mssparkutils.fs.put(f"{base_path}/drivers.json", json.dumps(requests.get(url_dr).json()), True)
        time.sleep(0.5) # <--- PAUSA OBLIGATORIA

        # ==========================================
        # 2. RESULTADOS Y SESIONES (Con validación y pausas)
        # ==========================================

        # A. Descargar TODAS las sesiones de este GP
        url_all_sessions = f"{BASE_URL}/sessions?meeting_key={meeting_key}"
        response_sessions = requests.get(url_all_sessions)
        time.sleep(0.5) # <--- PAUSA OBLIGATORIA
        
        # Validación de respuesta JSON
        try:
            sessions_data = response_sessions.json()
        except:
            print(f"   Warning: Could not decode JSON for {clean_name}")
            continue

        # Si nos bloquean, esperamos 5 segundos
        if isinstance(sessions_data, dict) and 'detail' in sessions_data:
             print(f"   Rate Limit Hit in {clean_name}. Waiting 5 seconds...")
             time.sleep(5)
             continue

        if not isinstance(sessions_data, list):
            print(f"   Skipping {clean_name}: Invalid response format.")
            continue

        mssparkutils.fs.put(f"{base_path}/sessions.json", json.dumps(sessions_data), True)

        # B. Descargar Resultados de cada sesión válida
        for session in sessions_data:
            s_name = session.get('session_name', 'Unknown') 
            s_key = session.get('session_key')

            if s_name in ["Race", "Sprint"] and s_key:
                url_pos = f"{BASE_URL}/position?session_key={s_key}"
                
                # Hacemos la petición y validamos
                resp_pos = requests.get(url_pos)
                
                if resp_pos.status_code == 200:
                    mssparkutils.fs.put(f"{base_path}/results_{s_name}.json", json.dumps(resp_pos.json()), True)
                else:
                    print(f"   Failed to get results for {s_name} (Status: {resp_pos.status_code})")
                
                time.sleep(0.5) # <--- PAUSA ENTRE CADA SESIÓN

        round_counter += 1
        
    except Exception as e:
        print(f"   Error in {clean_name}: {e}")

    # Pausa final entre GPs (por seguridad)
    time.sleep(1)

# --- 4. SAVE CIRCUITS MASTER ---
mssparkutils.fs.put(f"{PATH_BRONZE}/circuits.json", json.dumps(meetings), True)

print(f"\nINGESTION COMPLETE! Total Rounds: {round_counter - 1}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from notebookutils import mssparkutils

# --- CONFIGURACIÓN ---
BASE_URL = "https://api.openf1.org/v1"
DEBUG_PATH = "Files/debug/china_test"
YEAR = 2024

print(f"🧹 Limpiando carpeta de pruebas: {DEBUG_PATH}...")
try:
    mssparkutils.fs.rm(DEBUG_PATH, True)
except:
    pass

# --- 1. BUSCAR LA KEY DE CHINA (RACE) ---
print("🔍 Buscando el GP de China en la API...")

# Pedimos todas las carreras de 2024
response_races = requests.get(f"{BASE_URL}/sessions?year={YEAR}&session_type=Race")
races = response_races.json()

# Filtramos de forma segura usando .get() para evitar KeyErrors
china_race = None
for race in races:
    # Usamos .get() para que devuelva una cadena vacía '' si la clave no existe
    country = race.get('country_name', '')
    location = race.get('location', '')
    
    if "China" in country or "Shanghai" in location:
        china_race = race
        break

if not china_race:
    print("❌ ERROR: No se encontró el GP de China en la API.")
    print("   Listado de países encontrados:", [r.get('country_name') for r in races])
else:
    session_key = china_race['session_key']
    meeting_key = china_race['meeting_key']
    print(f"✅ GP Encontrado: {china_race.get('country_name')} (Key: {meeting_key})")
    print(f"🔑 Session Key (Race): {session_key}")

    # --- 2. DESCARGAR STANDINGS ESPECÍFICOS ---
    print(f"📥 Consultando puntos acumulados tras la carrera...")
    
    url_standings = f"{BASE_URL}/championship_drivers?session_key={session_key}"
    standings_data = requests.get(url_standings).json()
    
    # Guardamos el archivo para debug
    mssparkutils.fs.put(f"{DEBUG_PATH}/china_standings_debug.json", json.dumps(standings_data), True)
    print(f"💾 Archivo guardado en: {DEBUG_PATH}/china_standings_debug.json")

    # --- 3. EL VEREDICTO (¿100 o 110?) ---
    # Buscamos a Max (driver_number 1)
    max_verstappen = next((d for d in standings_data if d['driver_number'] == 1), None)

    if max_verstappen:
        puntos_api = max_verstappen.get('points')
        
        print("\n" + "="*50)
        print(f"🏎️  RESULTADO DEL ANÁLISIS PARA MAX VERSTAPPEN")
        print("="*50)
        print(f"📍 GP: China 2024")
        print(f"🔢 Puntos que devuelve la API HOY: {puntos_api}")
        print("-" * 30)
        
        if puntos_api == 100:
            print("⚠️  VEREDICTO: LA API SIGUE ROTA.")
            print("    El error es permanente en el origen.")
            print("👉  ACCIÓN RECOMENDADA: Usa el 'Parche Manual' (when meeting=1209 then 110).")
            
        elif puntos_api == 110:
            print("🎉  VEREDICTO: LA API ESTÁ CORREGIDA.")
            print("    Era solo un problema de timing en tu descarga anterior.")
            print("👉  ACCIÓN RECOMENDADA: Borra la carpeta de China en Bronze y re-ejecuta tu Ingesta.")
            
        else:
            print(f"🤔  DATO INESPERADO: {puntos_api}. Investiga manualmente.")
            
    else:
        print("❌ Error: No se encontró a Max Verstappen en la respuesta.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
