import csv
import os
import re
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(SCRIPT_DIR, "input-data")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output-data")

with open(os.path.join(INPUT_DIR, "states-list.txt"), encoding="utf-8") as f:
    states_list = [line.strip() for line in f if line.strip()]

states_set = set(states_list)

# parse Born field
# born format: "DD Month YYYY in City, State (COUNTRY)"
# we want the segment between the last comma and the country code in parens.
# matche criteria: ", State (ABC)" at the end of string
_BORN_RE = re.compile(r",\s*([^,]+?)\s*\(([A-Z]+)\)\s*$")


def parse_born(born_str: str):
    """Return (state, country_code) or (None, None) if not parseable."""
    if not born_str:
        return None, None
    m = _BORN_RE.search(born_str)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None, None


# load bios.csv and 2024-2026-us-bios.csv, merging and deduplicating by athlete_id

def load_bios(path: str) -> tuple[list, list[str]]:
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return rows, fieldnames


bios_rows, fieldnames = load_bios(os.path.join(INPUT_DIR, "bios.csv"))

new_bios_path = os.path.join(INPUT_DIR, "2024-2026-us-bios.csv")
if os.path.exists(new_bios_path):
    new_rows, new_fields = load_bios(new_bios_path)
    # merge fieldnames (preserve order, append any new columns)
    for col in new_fields:
        if col not in fieldnames:
            fieldnames.append(col)
    # deduplicate: index existing rows by athlete_id, then add new ones not yet seen
    seen_ids: set[str] = {str(r.get("athlete_id", "")) for r in bios_rows}
    added = 0
    for row in new_rows:
        aid = str(row.get("athlete_id", ""))
        if aid and aid not in seen_ids:
            bios_rows.append(row)
            seen_ids.add(aid)
            added += 1
    print(f"Loaded {new_bios_path}: {len(new_rows)} rows, {added} new athletes merged.")
else:
    print(f"[INFO] {new_bios_path} not found – using bios.csv only.")

athletes = bios_rows

# categorize athletes

# state name  →  list of athlete rows born in that state
state_athletes: dict[str, list] = defaultdict(list)

# athletes whose NOC contains "United States" but NOT born in a US state
us_noc_born_elsewhere: list = []

for athlete in athletes:
    born = athlete.get("Born", "")
    noc = athlete.get("NOC", "")

    state, country = parse_born(born)
    is_us_state_born = country == "USA" and state in states_set

    if is_us_state_born:
        state_athletes[state].append(athlete)

    # NOC field can hold multiple space-separated country names
    # (e.g. "People's Republic of China United States")
    # we check for the whole token "United States" as a substring.
    if "United States" in noc and not is_us_state_born:
        us_noc_born_elsewhere.append(athlete)

# write output files

os.makedirs(os.path.join(OUTPUT_DIR, "by-state"), exist_ok=True)


def write_csv(path: str, rows: list, fields):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


count_path = os.path.join(OUTPUT_DIR, "count.csv")
with open(count_path, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["state", "number"])
    for state in sorted(states_list):
        count = len(state_athletes.get(state, []))
        if count > 0:
            writer.writerow([state, count])

print(f"Wrote {count_path}")

for state, rows in state_athletes.items():
    # use the state name directly as the filename (macOS/Linux handle spaces fine).
    # replace any characters that could be problematic on some filesystems.
    safe_name = state.replace("/", "-")
    out_path = os.path.join(OUTPUT_DIR, "by-state", f"{safe_name}.csv")
    write_csv(out_path, rows, fieldnames)
    print(f"  Wrote {out_path}  ({len(rows)} athletes)")

all_us_athletes = []
for state in sorted(states_list):
    all_us_athletes.extend(state_athletes.get(state, []))

all_states_path = os.path.join(OUTPUT_DIR, "all-states.csv")
write_csv(all_states_path, all_us_athletes, fieldnames)
print(f"Wrote {all_states_path}  ({len(all_us_athletes)} total athletes)")

elsewhere_path = os.path.join(OUTPUT_DIR, "us-born-elsewhere.csv")
write_csv(elsewhere_path, us_noc_born_elsewhere, fieldnames)
print(f"Wrote {elsewhere_path}  ({len(us_noc_born_elsewhere)} athletes)")

# summary
print("\n--- Summary ---")
print(f"States with at least 1 athlete : {len(state_athletes)}")
print(f"Total US state-born athletes   : {len(all_us_athletes)}")
print(f"US NOC / born elsewhere         : {len(us_noc_born_elsewhere)}")
print("\nAthletes per state:")
for state in sorted(states_list):
    n = len(state_athletes.get(state, []))
    if n:
        print(f"  {state:<20} {n}")
