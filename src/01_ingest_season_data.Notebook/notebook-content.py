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

# A. Get all RACE sessions first (to map Meeting -> Session)
races_raw = requests.get(f"{BASE_URL}/sessions?year={YEAR}&session_type=Race").json()
# Create a lookup map: { meeting_key: session_key }
race_lookup = {r['meeting_key']: r['session_key'] for r in races_raw}

# B. Get all MEETINGS (Events)
# This is our main list to iterate. It is unique by definition.
meetings = requests.get(f"{BASE_URL}/meetings?year={YEAR}").json()
meetings_sorted = sorted(meetings, key=lambda x: x['date_start'])

print(f"Found {len(meetings_sorted)} scheduled events.")
print("Starting ingestion...\n")

# --- 3. MAIN LOOP (Events) ---
round_counter = 1

for meeting in meetings_sorted:
    meeting_key = meeting['meeting_key']
    official_name = meeting['meeting_name']
    
    # FILTER 1: Skip Testing
    if "Testing" in official_name:
        continue
        
    # FILTER 2: Must have a Race Session
    session_key = race_lookup.get(meeting_key)

    # --- PROCESSING ---
    # Name Cleaning: "Bahrain Grand Prix" -> "Bahrain_Grand_Prix"
    clean_name = official_name.replace(" ", "_")
    
    # Folder Structure: "01_Bahrain_Grand_Prix"
    folder_name = f"{str(round_counter).zfill(2)}_{clean_name}"
    base_path = f"{PATH_BRONZE}/{folder_name}"
    
    print(f"Round {round_counter}: {clean_name}")
    
    try:
        # Download Drivers Standings
        url_d = f"{BASE_URL}/championship_drivers?session_key={session_key}"
        mssparkutils.fs.put(f"{base_path}/drivers_standings.json", json.dumps(requests.get(url_d).json()), True)
        
        # Download Teams Standings
        url_t = f"{BASE_URL}/championship_teams?session_key={session_key}"
        mssparkutils.fs.put(f"{base_path}/teams_standings.json", json.dumps(requests.get(url_t).json()), True)

        # Download Drivers Roster
        url_dr = f"{BASE_URL}/drivers?session_key={session_key}"
        mssparkutils.fs.put(f"{base_path}/drivers.json", json.dumps(requests.get(url_dr).json()), True)
        
        # Only increment counter if successful
        round_counter += 1
        
    except Exception as e:
        print(f"   Error: {e}")

    time.sleep(0.2)

# --- 4. SAVE CIRCUITS MASTER ---
mssparkutils.fs.put(f"{PATH_BRONZE}/circuits.json", json.dumps(meetings), True)

print(f"\nINGESTION COMPLETE! Total Rounds: {round_counter - 1}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
