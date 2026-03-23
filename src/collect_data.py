import sys
sys.stdout.reconfigure(encoding='utf-8')
import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import quote

# ============================================================
#  CONFIG
# ============================================================
API_KEY  = "68646b6c69556c6b38366468747543"   # <-- replace with your key
BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE  = "citydata"

locations = [
    "광화문·덕수궁",
    "사당역",
    "서울대입구역",
    "노들섬",
    "어린이대공원"
]

# ============================================================
#  HELPER — extract one field safely
# ============================================================
def find_tag(root, tag):
    elem = root.find(f".//{tag}")
    return elem.text.strip() if elem is not None and elem.text else None

# ============================================================
#  MAIN LOOP
# ============================================================
rows = []

for location in locations:
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Calling: {location} ...")

    try:
        encoded = quote(location)
        url = f"{BASE_URL}/{API_KEY}/xml/{SERVICE}/1/5/{encoded}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.text)

        rows.append({
            "collected_at":          collected_at,
            "location_name":         location,
            "api_reported_time":     find_tag(root, "PPLTN_TIME"),
            "congestion_status_raw": find_tag(root, "AREA_CONGEST_LVL"),
            "congestion_msg":        find_tag(root, "AREA_CONGEST_MSG"),
            "congestion_level_3class": None,   # will fill in later
            "source_api":            SERVICE,
            "status_code":           response.status_code,
            "raw_response":          response.text[:5000]
        })

        print(f"  OK — congestion: {find_tag(root, 'AREA_CONGEST_LVL')}")

    except Exception as e:
        rows.append({
            "collected_at":          collected_at,
            "location_name":         location,
            "api_reported_time":     None,
            "congestion_status_raw": None,
            "congestion_msg":        None,
            "congestion_level_3class": None,
            "source_api":            SERVICE,
            "status_code":           None,
            "raw_response":          f"ERROR: {e}"
        })
        print(f"  ERROR: {e}")

# ============================================================
#  SAVE CSV
# ============================================================
df = pd.DataFrame(rows)

os.makedirs("data/raw", exist_ok=True)
filename = datetime.now().strftime("data/raw/seoul_citydata_%Y-%m-%d_%H-%M-%S.csv")
df.to_csv(filename, index=False, encoding="utf-8-sig")

print(f"\nSaved: {filename}")
print(f"Rows : {len(df)}")

# ============================================================
#  UPDATE DATA LOG
# ============================================================
log_path = "reports/weekly_logs/week4_data_log.csv"
os.makedirs("reports/weekly_logs", exist_ok=True)

success = all(df["status_code"] == 200)
log_row = pd.DataFrame([{
    "run_time":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "status":     "success" if success else "partial_error",
    "file_name":  filename,
    "rows_saved": len(df),
    "notes":      "all locations requested"
}])

if os.path.exists(log_path):
    log_row.to_csv(log_path, mode="a", header=False, index=False, encoding="utf-8-sig")
else:
    log_row.to_csv(log_path, index=False, encoding="utf-8-sig")

print(f"Log  : {log_path}")
