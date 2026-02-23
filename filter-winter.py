import csv
import os
import re
import time
import urllib.request
import urllib.parse
import urllib.error
from collections import defaultdict
from html.parser import HTMLParser

# config
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(SCRIPT_DIR, "input-data")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output-data")      # already-generated files
WINTER_DIR = os.path.join(SCRIPT_DIR, "winter-output-data")

# pauses to avoid rate limiting
REQUEST_DELAY = 4.0
MAX_DELAY     = 60.0
MAX_RETRIES   = 6

# ! IMPORTANT:
# Session cookie – update this if requests start returning 302/login pages.
# Grab a fresh value from browser DevTools → Network → any olympedia request
# → Cookie header → _olympedia_session=...
SESSION_COOKIE = (
    "VC9lRlJLakN2ejRvbHZIVkN1cXZYWmVGc2wrcm5jaVNTdU1rM0FvNW5kQ0hjeE5NRDN1"
    "cnZFeE8vT3hrYTMwVFBpRWZoL0d0NHVwdHkyajA1cTlmaGMrVndTR1MzZEdTTnBGR2ZT"
    "OGI2aXIyUE14L3k1K0g4UVhJNkxodHdSOGRsc1pEWWRjankrMWd1NHJjUHRZSXBBPT0t"
    "LTVPLzVQK2QzSXdqR1dYVzdZa2FQTkE9PQ%3D%3D--9182e32f652091bc293917961c4a"
    "a4d2176d5c04"
)

BASE_URL = "https://www.olympedia.org"

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "dnt": "1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "cookie": f"_olympedia_session={SESSION_COOKIE}",
}

# parse html 
class LinkTextParser(HTMLParser):
    """Collect text inside every <a> tag."""
    def __init__(self):
        super().__init__()
        self._in_a   = False
        self._buf    = []
        self.links   = []   # list of (href, text)
        self._href   = None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._in_a  = True
            self._buf   = []
            attrs_dict  = dict(attrs)
            self._href  = attrs_dict.get("href", "")

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self.links.append((self._href, "".join(self._buf).strip()))
            self._in_a = False
            self._buf  = []

    def handle_data(self, data):
        if self._in_a:
            self._buf.append(data)


_current_delay = REQUEST_DELAY  # module-level, reset on success


def _fetch(url: str) -> str | None:
    """GET url with exponential back-off on 429. Returns body or None."""
    global _current_delay
    delay = _current_delay
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(delay)
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                _current_delay = REQUEST_DELAY   # reset on success
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                delay = min(delay * 2, MAX_DELAY)
                print(f"  [429] rate-limited, backing off {delay:.0f}s "
                      f"(attempt {attempt}/{MAX_RETRIES}) → {url}")
            else:
                print(f"  [WARN] HTTP {exc.code} for {url}")
                return None
        except urllib.error.URLError as exc:
            print(f"  [WARN] fetch failed for {url}: {exc}")
            return None
    print(f"  [FAIL] max retries exceeded for {url}")
    return None

# check athlete page for Winter Olympics entries
def winter_games_from_page(athlete_id: str) -> list[str] | None:
    url  = f"{BASE_URL}/athletes/{athlete_id}"
    html = _fetch(url)
    if html is None:
        return None
    parser = LinkTextParser()
    parser.feed(html)
    games = []
    for href, text in parser.links:
        # links look like <a href="/editions/3">1904 Summer Olympics</a>
        if href.startswith("/editions/") and "Winter Olympics" in text:
            year_match = re.match(r"(\d{4} Winter Olympics)", text)
            if year_match and year_match.group(1) not in games:
                games.append(year_match.group(1))
    return sorted(games)


# for athletes without an ID, search by name and check if any results are winter olympians
def search_and_find_winter(used_name: str) -> list[str] | None:
    parts = used_name.replace("•", " ").split()
    if not parts:
        return None
    query = urllib.parse.quote_plus(" ".join(parts))
    url   = f"{BASE_URL}/athletes/quick_search?query={query}"
    html  = _fetch(url)
    if html is None:
        return None
    parser = LinkTextParser()
    parser.feed(html)
    for href, _text in parser.links:
        m = re.match(r"^/athletes/(\d+)$", href)
        if m:
            games = winter_games_from_page(m.group(1))
            if games is not None:
                return games
    return None


# load states
with open(os.path.join(INPUT_DIR, "states-list.txt"), encoding="utf-8") as f:
    states_list = [line.strip() for line in f if line.strip()]

# athlete_id -> winter games lookup from results.csv

print("Building winter-games lookup from results.csv …")

# keep games that are proper Winter Olympics (not Youth etc.)
_WINTER_RE = re.compile(r"^\d{4} Winter Olympics$")

winter_lookup: dict[str, list[str]] = defaultdict(list)
seen_ids: set[str] = set()   # every athlete_id that appears in results.csv

with open(os.path.join(INPUT_DIR, "results.csv"), encoding="utf-8", newline="") as f:
    for row in csv.DictReader(f):
        aid   = str(row.get("athlete_id", "")).strip()
        games = row.get("Games", "").strip()
        if not aid:
            continue
        seen_ids.add(aid)
        if _WINTER_RE.match(games) and games not in winter_lookup[aid]:
            winter_lookup[aid].append(games)

# sort each athlete's game list chronologically
for aid in winter_lookup:
    winter_lookup[aid].sort()

print(f"  {len(seen_ids):,} unique athlete IDs in results.csv")
print(f"  {len(winter_lookup):,} athletes with ≥1 Winter Olympics entry")

def make_out_fields(fieldnames: list[str]) -> list[str]:
    out = list(fieldnames)
    if "Roles" in out:
        out[out.index("Roles")] = "Games"
    elif "Games" not in out:
        out = ["Games"] + out
    return out


def build_out_row(row: dict, games_str: str, out_fields: list[str]) -> dict:
    out = {k: row.get(k, "") for k in out_fields}
    out["Games"] = games_str
    return out

def classify_athletes(athletes: list[dict], out_fields: list[str],
                      state_key_fn=None) -> tuple[list, list]:
    """
    state_key_fn(row) → string used for by-state grouping.
    If None, each row gets key "" (used for us-born-elsewhere).
    """
    winter_rows   = []   # (key, out_row)
    no_match_rows = []   # out_row
    needs_scrape  = []

    for row in athletes:
        aid = str(row.get("athlete_id", "")).strip()
        if aid in seen_ids:
            games = winter_lookup.get(aid, [])
            if games:
                key = state_key_fn(row) if state_key_fn else ""
                winter_rows.append(
                    (key, build_out_row(row, "; ".join(games), out_fields)))
            # else: has results but none are winter → not a winter olympian
        else:
            needs_scrape.append(row)

    if needs_scrape:
        total = len(needs_scrape)
        print(f"  {total} athletes not in results.csv – scraping olympedia.org …")
        for i, row in enumerate(needs_scrape, 1):
            aid       = str(row.get("athlete_id", "")).strip()
            used_name = row.get("Used name", "")
            print(f"  [{i}/{total}] {aid} ({used_name}) …", end=" ", flush=True)

            if aid:
                games = winter_games_from_page(aid)
            else:
                games = search_and_find_winter(used_name)

            if games is None:
                print("FAILED – adding to no-match")
                no_match_rows.append(build_out_row(row, "", out_fields))
            elif games:
                print(f"WINTER: {', '.join(games)}")
                key = state_key_fn(row) if state_key_fn else ""
                winter_rows.append(
                    (key, build_out_row(row, "; ".join(sorted(games)), out_fields)))
            else:
                print("not winter")

    return winter_rows, no_match_rows

def load_csv(path: str) -> tuple[list[str], list[dict]]:
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        return list(reader.fieldnames or []), rows  # fieldnames already consumed

# Re-open to get fieldnames properly
def load_csv2(path: str):
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


print("Loading source CSVs …")
all_states_fields, all_states_athletes = load_csv2(
    os.path.join(OUTPUT_DIR, "all-states.csv"))
elsewhere_fields, elsewhere_athletes   = load_csv2(
    os.path.join(OUTPUT_DIR, "us-born-elsewhere.csv"))

out_fields = make_out_fields(all_states_fields)

# extract born state for grouping (reuse the same regex as olympians-by-state)
_BORN_RE = re.compile(r",\s*([^,]+?)\s*\(([A-Z]+)\)\s*$")

def born_state(row: dict) -> str:
    born = row.get("Born", "")
    m    = _BORN_RE.search(born)
    return m.group(1).strip() if m else "Unknown"

print("\nClassifying US state-born athletes …")
winter_rows, no_match_rows = classify_athletes(
    all_states_athletes, out_fields, state_key_fn=born_state)

print("\nClassifying US-NOC / born-elsewhere athletes …")
winter_elsewhere, no_match_elsewhere = classify_athletes(
    elsewhere_athletes, out_fields, state_key_fn=None)

# by-state grouping from state-born athletes
state_winter: dict[str, list] = defaultdict(list)
for key, row in winter_rows:
    state_winter[key].append(row)

os.makedirs(os.path.join(WINTER_DIR, "by-state"), exist_ok=True)


def write_csv(path: str, rows: list, fields: list):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


count_path = os.path.join(WINTER_DIR, "count.csv")
with open(count_path, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["state", "number"])
    for state in sorted(states_list):
        n = len(state_winter.get(state, []))
        if n > 0:
            writer.writerow([state, n])
print(f"\nWrote {count_path}")

for state in sorted(states_list):
    rows = state_winter.get(state, [])
    if not rows:
        continue
    safe_name = state.replace("/", "-")
    out_path  = os.path.join(WINTER_DIR, "by-state", f"{safe_name}.csv")
    write_csv(out_path, rows, out_fields)
    print(f"  Wrote {out_path}  ({len(rows)} athletes)")


all_winter = []
for state in sorted(states_list):
    all_winter.extend(state_winter.get(state, []))

all_path = os.path.join(WINTER_DIR, "all-states.csv")
write_csv(all_path, all_winter, out_fields)
print(f"Wrote {all_path}  ({len(all_winter)} athletes)")

elsewhere_out = [row for _, row in winter_elsewhere]
elsewhere_path = os.path.join(WINTER_DIR, "us-born-elsewhere.csv")
write_csv(elsewhere_path, elsewhere_out, make_out_fields(elsewhere_fields))
print(f"Wrote {elsewhere_path}  ({len(elsewhere_out)} athletes)")

all_no_match = no_match_rows + no_match_elsewhere
no_match_path = os.path.join(WINTER_DIR, "no-match.csv")
write_csv(no_match_path, all_no_match, out_fields)
print(f"Wrote {no_match_path}  ({len(all_no_match)} athletes)")

# summary
print("\n--- Summary ---")
print(f"States with ≥1 winter athlete  : {sum(1 for s in states_list if state_winter.get(s))}")
print(f"Total winter / state-born      : {len(all_winter)}")
print(f"Winter / US NOC born elsewhere : {len(elsewhere_out)}")
print(f"No-match (scrape needed/failed): {len(all_no_match)}")
print("\nWinter athletes per state:")
for state in sorted(states_list):
    n = len(state_winter.get(state, []))
    if n:
        print(f"  {state:<20} {n}")
