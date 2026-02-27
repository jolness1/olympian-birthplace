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
SUMMER_DIR = os.path.join(SCRIPT_DIR, "summer-output-data")

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

# check athlete page for Olympics entries of a given season ("Winter" or "Summer")
def games_from_page(athlete_id: str, season: str) -> list[str] | None:
    url  = f"{BASE_URL}/athletes/{athlete_id}"
    html = _fetch(url)
    if html is None:
        return None
    parser = LinkTextParser()
    parser.feed(html)
    games = []
    label = f"{season} Olympics"
    for href, text in parser.links:
        # links look like <a href="/editions/3">1904 Summer Olympics</a>
        if href.startswith("/editions/") and label in text:
            year_match = re.match(rf"(\d{{4}} {season} Olympics)", text)
            if year_match and int(year_match.group(1)[:4]) >= MIN_YEAR and year_match.group(1) not in games:
                games.append(year_match.group(1))
    return sorted(games)


# for athletes without an ID, search by name and check if any results match the season
def search_and_find_season(used_name: str, season: str) -> list[str] | None:
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
            games = games_from_page(m.group(1), season)
            if games is not None:
                return games
    return None


# load states
with open(os.path.join(INPUT_DIR, "states-list.txt"), encoding="utf-8") as f:
    states_list = [line.strip() for line in f if line.strip()]

# athlete_id -> winter games lookup from results.csv

print("Building season-games lookups from results.csv …")

# keep games that are proper Olympics only (not Youth etc.)
_WINTER_RE = re.compile(r"^\d{4} Winter Olympics$")
_SUMMER_RE = re.compile(r"^\d{4} Summer Olympics$")
MIN_YEAR   = 1924   # Winter Olympics began in 1924; exclude earlier games

winter_lookup: dict[str, list[str]] = defaultdict(list)
summer_lookup: dict[str, list[str]] = defaultdict(list)
seen_ids: set[str] = set()   # every athlete_id that appears in results.csv

with open(os.path.join(INPUT_DIR, "results.csv"), encoding="utf-8", newline="") as f:
    for row in csv.DictReader(f):
        aid   = str(row.get("athlete_id", "")).strip()
        games = row.get("Games", "").strip()
        if not aid:
            continue
        seen_ids.add(aid)
        if _WINTER_RE.match(games) and int(games[:4]) >= MIN_YEAR and games not in winter_lookup[aid]:
            winter_lookup[aid].append(games)
        if _SUMMER_RE.match(games) and int(games[:4]) >= MIN_YEAR and games not in summer_lookup[aid]:
            summer_lookup[aid].append(games)

# sort each athlete's game list chronologically
for aid in winter_lookup:
    winter_lookup[aid].sort()
for aid in summer_lookup:
    summer_lookup[aid].sort()

print(f"  {len(seen_ids):,} unique athlete IDs in results.csv")
print(f"  {len(winter_lookup):,} athletes with ≥1 Winter Olympics entry")
print(f"  {len(summer_lookup):,} athletes with ≥1 Summer Olympics entry")

# also load 2024-2026-us-results.csv if present
_new_results = os.path.join(INPUT_DIR, "2024-2026-us-results.csv")
if os.path.exists(_new_results):
    _before = len(seen_ids)
    with open(_new_results, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            aid   = str(row.get("athlete_id", "")).strip()
            games = row.get("Games", "").strip()
            if not aid:
                continue
            seen_ids.add(aid)
            if _WINTER_RE.match(games) and int(games[:4]) >= MIN_YEAR and games not in winter_lookup[aid]:
                winter_lookup[aid].append(games)
            if _SUMMER_RE.match(games) and int(games[:4]) >= MIN_YEAR and games not in summer_lookup[aid]:
                summer_lookup[aid].append(games)
    for aid in winter_lookup:
        winter_lookup[aid].sort()
    for aid in summer_lookup:
        summer_lookup[aid].sort()
    print(f"Merged {_new_results}: +{len(seen_ids) - _before} new athlete IDs")
else:
    print(f"[INFO] {_new_results} not found – using results.csv only.")

def build_output_cache() -> dict[str, dict[str, list[str]]]:
    """
    Read any already-written season output files so we can skip re-scraping
    athletes that were classified in a previous run.

    Returns {athlete_id: {"Winter": [games…], "Summer": [games…]}}.
    An entry with an empty games list means the athlete was previously found
    to have no games for that season (scraped & confirmed negative).
    Athletes in no-match.csv are recorded with an empty list so they are
    re-attempted by default (their previous scrape may have been a network
    failure).
    """
    cache: dict[str, dict[str, list[str]]] = {}

    def _add(aid: str, season: str, games: list[str]) -> None:
        if aid not in cache:
            cache[aid] = {"Winter": None, "Summer": None}  # None = unseen
        existing = cache[aid][season]
        if existing is None:
            cache[aid][season] = sorted(games)
        else:
            merged = sorted(set(existing) | set(games))
            cache[aid][season] = merged

    for out_dir, season in ((WINTER_DIR, "Winter"), (SUMMER_DIR, "Summer")):
        # Positive results: athletes that DID compete this season
        for fname in ("all-states.csv", "us-born-elsewhere.csv"):
            path = os.path.join(out_dir, fname)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    aid = str(row.get("athlete_id", "")).strip()
                    if not aid:
                        continue
                    games_str = row.get("Games", "").strip()
                    games = [g.strip() for g in games_str.split(";") if g.strip()]
                    _add(aid, season, games)
        # Negative results: athletes with NO games this season (scraped, not found)
        # Store empty list so classify_athletes skips re-scraping them.
        # Note: no-match.csv contains FAILED scrapes – leave those as None so
        # they are retried.
        # We infer negatives by marking every athlete in seen_ids that has a
        # cache entry for the other season but not this one — handled implicitly
        # below; nothing explicit to load here for negatives.

    return cache


print("Building output cache from previous runs …")
_output_cache = build_output_cache()
_cached_total = sum(
    1 for v in _output_cache.values()
    if v.get("Winter") is not None or v.get("Summer") is not None
)
print(f"  {_cached_total:,} athletes found in existing output files (will skip API)")


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
                      season: str, lookup: dict,
                      state_key_fn=None) -> tuple[list, list]:
    """
    season       : "Winter" or "Summer"
    lookup       : pre-built {athlete_id: [games]} dict for this season
    state_key_fn : row → string used for by-state grouping.
                   If None, each row gets key "" (used for us-born-elsewhere).
    """
    season_rows   = []   # (key, out_row)
    no_match_rows = []   # out_row
    needs_scrape  = []
    from_cache    = 0

    for row in athletes:
        aid = str(row.get("athlete_id", "")).strip()
        if aid in seen_ids:
            # ID is in results — use pre-built lookup
            games = lookup.get(aid, [])
            if games:
                key = state_key_fn(row) if state_key_fn else ""
                season_rows.append(
                    (key, build_out_row(row, "; ".join(games), out_fields)))
            # else: has results but none match this season → skip
        elif _output_cache.get(aid, {}).get(season) is not None:
            # Already classified in a previous run — reuse without hitting API
            games = _output_cache[aid][season]
            from_cache += 1
            if games:
                key = state_key_fn(row) if state_key_fn else ""
                season_rows.append(
                    (key, build_out_row(row, "; ".join(sorted(games)), out_fields)))
            # else: was scraped before and confirmed no games this season → skip
        else:
            needs_scrape.append(row)

    if from_cache:
        print(f"  {from_cache} athletes resolved from output cache (no API call)")
    if not needs_scrape:
        print(f"  No athletes required API scraping for {season}.")
        return season_rows, no_match_rows

    if needs_scrape:
        total = len(needs_scrape)
        print(f"  {total} athletes not in results.csv – scraping olympedia.org …")
        for i, row in enumerate(needs_scrape, 1):
            aid       = str(row.get("athlete_id", "")).strip()
            used_name = row.get("Used name", "")
            print(f"  [{i}/{total}] {aid} ({used_name}) …", end=" ", flush=True)

            if aid:
                games = games_from_page(aid, season)
            else:
                games = search_and_find_season(used_name, season)

            if games is None:
                print("FAILED – adding to no-match")
                no_match_rows.append(build_out_row(row, "", out_fields))
            elif games:
                print(f"{season.upper()}: {', '.join(games)}")
                key = state_key_fn(row) if state_key_fn else ""
                season_rows.append(
                    (key, build_out_row(row, "; ".join(sorted(games)), out_fields)))
            else:
                print(f"not {season.lower()}")

    return season_rows, no_match_rows

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

def write_csv(path: str, rows: list, fields: list):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_season_output(season: str, out_dir: str, lookup: dict,
                        states_rows: list, elsewhere_rows: list,
                        no_match_all: list, out_fields: list,
                        elsewhere_fields: list):
    """Write all output files for one season into out_dir."""
    os.makedirs(os.path.join(out_dir, "by-state"), exist_ok=True)

    # group by state
    state_season: dict[str, list] = defaultdict(list)
    for key, row in states_rows:
        state_season[key].append(row)

    count_path = os.path.join(out_dir, "count.csv")
    with open(count_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["state", "number"])
        for state in sorted(states_list):
            n = len(state_season.get(state, []))
            if n > 0:
                writer.writerow([state, n])
    print(f"  Wrote {count_path}")

    for state in sorted(states_list):
        rows = state_season.get(state, [])
        if not rows:
            continue
        safe_name = state.replace("/", "-")
        out_path  = os.path.join(out_dir, "by-state", f"{safe_name}.csv")
        write_csv(out_path, rows, out_fields)
        print(f"    Wrote {out_path}  ({len(rows)} athletes)")

    all_season = []
    for state in sorted(states_list):
        all_season.extend(state_season.get(state, []))
    all_path = os.path.join(out_dir, "all-states.csv")
    write_csv(all_path, all_season, out_fields)
    print(f"  Wrote {all_path}  ({len(all_season)} athletes)")

    elsewhere_out  = [row for _, row in elsewhere_rows]
    elsewhere_path = os.path.join(out_dir, "us-born-elsewhere.csv")
    write_csv(elsewhere_path, elsewhere_out, make_out_fields(elsewhere_fields))
    print(f"  Wrote {elsewhere_path}  ({len(elsewhere_out)} athletes)")

    no_match_path = os.path.join(out_dir, "no-match.csv")
    write_csv(no_match_path, no_match_all, out_fields)
    print(f"  Wrote {no_match_path}  ({len(no_match_all)} athletes)")

    label = season.lower()
    print(f"\n--- {season} Summary ---")
    print(f"States with ≥1 {label} athlete  : {sum(1 for s in states_list if state_season.get(s))}")
    print(f"Total {label} / state-born      : {len(all_season)}")
    print(f"{season} / US NOC born elsewhere : {len(elsewhere_out)}")
    print(f"No-match (scrape needed/failed) : {len(no_match_all)}")
    print(f"\n{season} athletes per state:")
    for state in sorted(states_list):
        n = len(state_season.get(state, []))
        if n:
            print(f"  {state:<20} {n}")

    return all_season, elsewhere_out


# ── Winter ────────────────────────────────────────────────────────────────────
print("\nClassifying Winter – US state-born athletes …")
winter_rows, no_match_rows = classify_athletes(
    all_states_athletes, out_fields, season="Winter", lookup=winter_lookup,
    state_key_fn=born_state)

print("\nClassifying Winter – US-NOC / born-elsewhere athletes …")
winter_elsewhere, no_match_elsewhere = classify_athletes(
    elsewhere_athletes, out_fields, season="Winter", lookup=winter_lookup,
    state_key_fn=None)

print("\nWriting Winter output …")
write_season_output(
    season="Winter",
    out_dir=WINTER_DIR,
    lookup=winter_lookup,
    states_rows=winter_rows,
    elsewhere_rows=winter_elsewhere,
    no_match_all=no_match_rows + no_match_elsewhere,
    out_fields=out_fields,
    elsewhere_fields=elsewhere_fields,
)

# ── Summer ────────────────────────────────────────────────────────────────────
print("\nClassifying Summer – US state-born athletes …")
summer_rows, no_match_rows_s = classify_athletes(
    all_states_athletes, out_fields, season="Summer", lookup=summer_lookup,
    state_key_fn=born_state)

print("\nClassifying Summer – US-NOC / born-elsewhere athletes …")
summer_elsewhere, no_match_elsewhere_s = classify_athletes(
    elsewhere_athletes, out_fields, season="Summer", lookup=summer_lookup,
    state_key_fn=None)

print("\nWriting Summer output …")
write_season_output(
    season="Summer",
    out_dir=SUMMER_DIR,
    lookup=summer_lookup,
    states_rows=summer_rows,
    elsewhere_rows=summer_elsewhere,
    no_match_all=no_match_rows_s + no_match_elsewhere_s,
    out_fields=out_fields,
    elsewhere_fields=elsewhere_fields,
)
