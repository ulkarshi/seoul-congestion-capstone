import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import os
import time
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import pandas as pd
import requests


# ============================================================
#  TIMEZONE
# ============================================================
KST = ZoneInfo("Asia/Seoul")


# ============================================================
#  CONFIG
# ============================================================
API_KEY = "68646b6c69556c6b38366468747543"
BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE = "citydata"

locations = [
    "광화문·덕수궁",
    "사당역",
    "서울대입구역",
    "노들섬",
    "어린이대공원",
]

DATA_DIR = Path("data/raw")
LOG_PATH = Path("reports/weekly_logs/week4_data_log.csv")


# ============================================================
#  LOGGING HELPERS
# ============================================================
def append_weekly_log(run_time: str, status: str, file_name: str, rows_saved: int, notes: str) -> None:
    os.makedirs(LOG_PATH.parent, exist_ok=True)

    log_row = pd.DataFrame([{
        "run_time": run_time,
        "status": status,
        "file_name": file_name,
        "rows_saved": rows_saved,
        "notes": notes
    }])

    if LOG_PATH.exists():
        log_row.to_csv(LOG_PATH, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        log_row.to_csv(LOG_PATH, index=False, encoding="utf-8-sig")


# ============================================================
#  CHECK: already saved this hour?
# ============================================================
def already_saved_this_hour() -> bool:
    now = datetime.now(KST)
    hour_prefix = now.strftime("seoul_citydata_%Y-%m-%d_%H-")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    existing = list(DATA_DIR.glob(f"{hour_prefix}*.csv"))
    return len(existing) > 0


# ============================================================
#  HELPER — extract one field safely
# ============================================================
def find_tag(root: ET.Element, tag: str):
    elem = root.find(f".//{tag}")
    return elem.text.strip() if elem is not None and elem.text else None


# ============================================================
#  HTTP FETCH WITH RETRY
# ============================================================
def fetch_with_retry(url: str, retries: int = 3, delay: int = 10):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"Request attempt {attempt}/{retries}")
            response = requests.get(url, timeout=30)
            print(f"Response status: {getattr(response, 'status_code', 'no response')}")
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout as e:
            last_error = e
            print(f"Attempt {attempt}/{retries} timeout: {repr(e)}")

        except requests.exceptions.HTTPError as e:
            last_error = e
            status = getattr(e.response, "status_code", None)
            print(f"Attempt {attempt}/{retries} http error: {status} {repr(e)}")

        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"Attempt {attempt}/{retries} request failed: {repr(e)}")

        except Exception as e:
            last_error = e
            print(f"Attempt {attempt}/{retries} failed: {repr(e)}")

        if attempt < retries:
            print(f"Sleeping {delay}s before retry...")
            time.sleep(delay)

    raise last_error


# ============================================================
#  START LOG
# ============================================================
workflow_started_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
print(f"Workflow started at: {workflow_started_at}")
print(f"GITHUB_RUN_ID: {os.getenv('GITHUB_RUN_ID', 'local')}")
print(f"GITHUB_EVENT_NAME: {os.getenv('GITHUB_EVENT_NAME', 'local')}")


# ============================================================
#  SKIP IF THIS HOUR ALREADY SAVED
# ============================================================
if already_saved_this_hour():
    print("Data already saved for this hour. Exiting.")
    append_weekly_log(
        run_time=datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        status="skipped",
        file_name="",
        rows_saved=0,
        notes="already saved this hour"
    )
    raise SystemExit(0)


# ============================================================
#  MAIN LOOP
# ============================================================
rows = []
notes_list = []

for location in locations:
    collected_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Calling: {location} ...")

    url = None
    response = None

    try:
        encoded = quote(location)
        url = f"{BASE_URL}/{API_KEY}/xml/{SERVICE}/1/5/{encoded}"
        response = fetch_with_retry(url, retries=3, delay=10)

        print(f"URL: {url}")
        print(f"Status: {getattr(response, 'status_code', 'no response')}")

        try:
            root = ET.fromstring(response.text)
        except ET.ParseError as e:
            print(f"URL: {url}")
            print(f"Status: {getattr(response, 'status_code', 'no response')}")
            print(f"ERROR for {location}: XML parse error: {repr(e)}")
            raise

        api_reported_time = find_tag(root, "PPLTN_TIME")
        congestion_raw = find_tag(root, "AREA_CONGEST_LVL")
        congestion_msg = find_tag(root, "AREA_CONGEST_MSG")

        rows.append({
            "collected_at": collected_at,
            "location_name": location,
            "api_reported_time": api_reported_time,
            "congestion_status_raw": congestion_raw,
            "congestion_msg": congestion_msg,
            "congestion_level_3class": None,
            "source_api": SERVICE,
            "status_code": getattr(response, "status_code", None),
            "raw_response": getattr(response, "text", "")[:5000]
        })

        print(f"  OK — congestion: {congestion_raw}")
        notes_list.append(f"success on {location}")

    except Exception as e:
        print(f"URL: {url}")
        print(f"Status: {getattr(response, 'status_code', 'no response')}")
        print(f"ERROR for {location}: {repr(e)}")

        rows.append({
            "collected_at": collected_at,
            "location_name": location,
            "api_reported_time": None,
            "congestion_status_raw": None,
            "congestion_msg": None,
            "congestion_level_3class": None,
            "source_api": SERVICE,
            "status_code": getattr(response, "status_code", None),
            "raw_response": (
                getattr(response, "text", "")[:5000]
                if response is not None
                else f"ERROR: {repr(e)}"
            )
        })

        err_text = repr(e)
        if isinstance(e, ET.ParseError) or "ParseError" in err_text:
            notes_list.append(f"xml parse error on {location}")
        elif "Timeout" in err_text:
            notes_list.append(f"timeout on {location}")
        else:
            notes_list.append(f"error on {location}: {err_text}")


# ============================================================
#  SAVE CSV
# ============================================================
df = pd.DataFrame(rows)

os.makedirs(DATA_DIR, exist_ok=True)
filename = datetime.now(KST).strftime("data/raw/seoul_citydata_%Y-%m-%d_%H-%M-%S.csv")
df.to_csv(filename, index=False, encoding="utf-8-sig")

print(f"\nSaved: {filename}")
print(f"Rows : {len(df)}")


# ============================================================
#  UPDATE WEEKLY LOG
# ============================================================
success_count = sum(1 for r in rows if r["congestion_status_raw"] is not None)
error_count = len(rows) - success_count

if success_count == len(locations):
    final_status = "success"
    final_notes = f"all {len(locations)} success"
elif success_count == 0:
    final_status = "failed"
    final_notes = "; ".join(notes_list)
else:
    final_status = "partial_error"
    final_notes = "; ".join(notes_list)

append_weekly_log(
    run_time=datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
    status=final_status,
    file_name=filename,
    rows_saved=len(df),
    notes=final_notes
)

print(f"Success rows: {success_count}")
print(f"Error rows  : {error_count}")
print(f"Log  : {LOG_PATH}")