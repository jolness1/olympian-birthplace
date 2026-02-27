import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(SCRIPT_DIR, "input-data")
STATE_CSV  = os.path.join(SCRIPT_DIR, "output-data", "by-state", "Montana.csv")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "extra-data")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "montana-events.csv")

OUT_FIELDS = ["Games", "Event", "Team", "Pos", "Medal", "As",
              "athlete_id", "NOC", "Discipline", "Nationality", "Unnamed: 7"]

# MT athlete IDs
montana_ids: set[str] = set()
with open(STATE_CSV, encoding="utf-8", newline="") as f:
    for row in csv.DictReader(f):
        aid = str(row.get("athlete_id", "")).strip()
        if aid:
            montana_ids.add(aid)

print(f"Montana athlete IDs loaded: {len(montana_ids)}")

results_files = [
    os.path.join(INPUT_DIR, "results.csv"),
    os.path.join(INPUT_DIR, "2024-2026-us-results.csv"),
]

matched_rows: list[dict] = []

for path in results_files:
    if not os.path.exists(path):
        print(f"[WARN] not found, skipping: {path}")
        continue
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            aid = str(row.get("athlete_id", "")).strip()
            if aid in montana_ids:
                out_row = {field: row.get(field, "") for field in OUT_FIELDS}
                matched_rows.append(out_row)
    print(f"  Scanned {path}")

print(f"Total matching rows: {len(matched_rows)}")

os.makedirs(OUTPUT_DIR, exist_ok=True)
with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=OUT_FIELDS)
    writer.writeheader()
    writer.writerows(matched_rows)

print(f"Written to {OUTPUT_CSV}")
