import csv
import os

WINTER_COUNT  = os.path.join("winter-output-data", "count.csv")
SUMMER_COUNT  = os.path.join("summer-output-data", "count.csv")
CENSUS        = os.path.join("input-data", "census-2020.csv")
OUT_DIR       = "per-capita"

os.makedirs(OUT_DIR, exist_ok=True)


def load_census(path: str) -> dict[str, int]:
    with open(path, encoding="utf-8", newline="") as f:
        return {row["state"]: int(row["population"]) for row in csv.DictReader(f)}


def load_count(path: str) -> dict[str, int]:
    with open(path, encoding="utf-8", newline="") as f:
        return {row["state"]: int(row["number"]) for row in csv.DictReader(f)}


def per_100k(count: int, population: int) -> float:
    return round(count / population * 100_000, 4)


population = load_census(CENSUS)
winter     = load_count(WINTER_COUNT)
summer     = load_count(SUMMER_COUNT)

# all states present in either count file, sorted alphabetically
all_states = sorted(set(winter) | set(summer))

# compute US totals (sum of census populations and counts)
pop_total = sum(population.values())
total_winter = sum(winter.values())
total_summer = sum(summer.values())

# --- winter-count.csv ---
with open(os.path.join(OUT_DIR, "winter-count.csv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["state", "per100kResidents"])
    for state in all_states:
        if state not in winter:
            continue
        pop = population.get(state)
        if pop is None:
            print(f"[WARN] no census data for '{state}' — skipping")
            continue
        w.writerow([state, per_100k(winter[state], pop)])
    # United States aggregate
    if pop_total > 0:
        w.writerow(["United States", per_100k(total_winter, pop_total)])

# --- summer-count.csv ---
with open(os.path.join(OUT_DIR, "summer-count.csv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["state", "per100kResidents"])
    for state in all_states:
        if state not in summer:
            continue
        pop = population.get(state)
        if pop is None:
            print(f"[WARN] no census data for '{state}' — skipping")
            continue
        w.writerow([state, per_100k(summer[state], pop)])
    # United States aggregate
    if pop_total > 0:
        w.writerow(["United States", per_100k(total_summer, pop_total)])

# --- combined-count.csv ---
with open(os.path.join(OUT_DIR, "combined-count.csv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["state", "winterPer100kResidents", "summerPer100kResidents"])
    for state in all_states:
        pop = population.get(state)
        if pop is None:
            print(f"[WARN] no census data for '{state}' — skipping")
            continue
        winter_val = per_100k(winter.get(state, 0), pop)
        summer_val = per_100k(summer.get(state, 0), pop)
        w.writerow([state, winter_val, summer_val])
    # United States aggregate
    if pop_total > 0:
        w.writerow(["United States", per_100k(total_winter, pop_total), per_100k(total_summer, pop_total)])

# --- overall-count.csv ---
with open(os.path.join(OUT_DIR, "overall-count.csv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["state", "per100kResidents"])
    for state in all_states:
        pop = population.get(state)
        if pop is None:
            print(f"[WARN] no census data for '{state}' — skipping")
            continue
        total = winter.get(state, 0) + summer.get(state, 0)
        w.writerow([state, per_100k(total, pop)])
    # United States aggregate (total = winter + summer)
    if pop_total > 0:
        w.writerow(["United States", per_100k(total_winter + total_summer, pop_total)])

print(f"Wrote {len(all_states)} states to {OUT_DIR}/")

# --- mergedTop10.csv: top 10 states by overall per100kResidents + US averages ---
merged_path = os.path.join(OUT_DIR, "mergedTop10.csv")
rows = []
for state in all_states:
    pop = population.get(state)
    if pop is None:
        continue
    winter_val = per_100k(winter.get(state, 0), pop)
    summer_val = per_100k(summer.get(state, 0), pop)
    overall = per_100k(winter.get(state, 0) + summer.get(state, 0), pop)
    rows.append((state, winter_val, summer_val, overall))

# sort descending by overall and take top 10
top10 = sorted(rows, key=lambda r: r[3], reverse=True)[:10]

# US averages (as one extra row)
us_winter = per_100k(total_winter, pop_total) if pop_total > 0 else 0
us_summer = per_100k(total_summer, pop_total) if pop_total > 0 else 0
us_overall = per_100k(total_winter + total_summer, pop_total) if pop_total > 0 else 0

with open(merged_path, "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["state", "winterPer100kResidents", "summerPer100kResidents", "per100kResidents"])
    for state, wv, sv, ov in top10:
        w.writerow([state, wv, sv, ov])
    # append US averages as final row
    w.writerow(["United States", us_winter, us_summer, us_overall])

print(f"Wrote merged top-10 to {merged_path}")
