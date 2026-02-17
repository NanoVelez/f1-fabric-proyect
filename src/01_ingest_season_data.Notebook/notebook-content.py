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

YEAR = "2023"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from notebookutils import mssparkutils
import time

# --- CONFIGURATION ---

# 0. Spark Configuration
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

BASE_URL = "https://api.openf1.org/v1"
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

# B. Get all MEETINGS
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
        # 1. GENERAL DOWNLOADS (With pauses)
        # ==========================================
        
        # Drivers Standings
        url_d = f"{BASE_URL}/championship_drivers?session_key={session_key_race}"
        mssparkutils.fs.put(f"{base_path}/drivers_standings.json", json.dumps(requests.get(url_d).json()), True)
        time.sleep(0.5) 

        # Teams Standings
        url_t = f"{BASE_URL}/championship_teams?session_key={session_key_race}"
        mssparkutils.fs.put(f"{base_path}/teams_standings.json", json.dumps(requests.get(url_t).json()), True)
        time.sleep(0.5) 

        # Drivers Roster
        url_dr = f"{BASE_URL}/drivers?session_key={session_key_race}"
        mssparkutils.fs.put(f"{base_path}/drivers.json", json.dumps(requests.get(url_dr).json()), True)
        time.sleep(0.5) 

        # ==========================================
        # 2. RESULTS AND SESSIONS (With validation and pauses)
        # ==========================================

        # A. Download ALL sessions for this GP
        url_all_sessions = f"{BASE_URL}/sessions?meeting_key={meeting_key}"
        response_sessions = requests.get(url_all_sessions)
        time.sleep(0.5) # <--- MANDATORY PAUSE
        
        # JSON response validation
        try:
            sessions_data = response_sessions.json()
        except:
            print(f"   Warning: Could not decode JSON for {clean_name}")
            continue

        # If we get blocked, wait 5 seconds
        if isinstance(sessions_data, dict) and 'detail' in sessions_data:
             print(f"   Rate Limit Hit in {clean_name}. Waiting 5 seconds...")
             time.sleep(5)
             continue

        if not isinstance(sessions_data, list):
            print(f"   Skipping {clean_name}: Invalid response format.")
            continue

        mssparkutils.fs.put(f"{base_path}/sessions.json", json.dumps(sessions_data), True)

        # B. Download Results for each valid session
        for session in sessions_data:
            s_name = session.get('session_name', 'Unknown') 
            s_key = session.get('session_key')

            if s_name in ["Race", "Sprint"] and s_key:
                url_pos = f"{BASE_URL}/position?session_key={s_key}"
                
                # Make request and validate
                resp_pos = requests.get(url_pos)
                
                if resp_pos.status_code == 200:
                    mssparkutils.fs.put(f"{base_path}/results_{s_name}.json", json.dumps(resp_pos.json()), True)
                else:
                    print(f"   Failed to get results for {s_name} (Status: {resp_pos.status_code})")
                
                time.sleep(0.5) # <--- PAUSE BETWEEN EACH SESSION

        round_counter += 1
        
    except Exception as e:
        print(f"   Error in {clean_name}: {e}")

    # Final pause between GPs
    time.sleep(1)

# --- 4. SAVE CIRCUITS MASTER ---
mssparkutils.fs.put(f"{PATH_BRONZE}/circuits.json", json.dumps(meetings), True)

print(f"\nINGESTION COMPLETE! Total Rounds: {round_counter - 1}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
