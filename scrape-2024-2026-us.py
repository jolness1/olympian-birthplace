"""
Scraper for US Olympic athletes from the 2024 Summer and 2026 Winter Olympics.

Outputs:
  input-data/2024-2026-us-bios.csv    – same columns as bios.csv
  input-data/2024-2026-us-results.csv – same columns as results.csv

Strategy:
  1. Scrape /countries/USA to find edition IDs for the two target games.
  2. Scrape /countries/USA/editions/{id} for each edition to collect athlete IDs.
  3. For each unique athlete_id, scrape /athletes/{id} for bio + results.
  4. Write the two output CSVs.

Session cookie:
  Update SESSION_COOKIE if you get login redirects.
  DevTools → Network → any olympedia request → Cookie header →
  _olympedia_session=...
"""

import csv
import os
import re
import time
import sys
from io import StringIO

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(SCRIPT_DIR, "input-data")
os.makedirs(INPUT_DIR, exist_ok=True)

BIOS_OUT    = os.path.join(INPUT_DIR, "2024-2026-us-bios.csv")
RESULTS_OUT = os.path.join(INPUT_DIR, "2024-2026-us-results.csv")

BASE_URL = "https://www.olympedia.org"

# Paste a fresh _olympedia_session value here if you hit login redirects
SESSION_COOKIE = (
    "dmR1bjlha3YxaGUwamJDZkZaQ3FSNC9pRGNXZzNBUjdtUlEvV2w2MVhvaXowOGQwUnpO"
    "eUh6MThSYkdCTUtLNUNUYjNySjlZY3RidVJxbG5sdG91YmxQTm41K2RvY1dyZ3EwV0pO"
    "Um9FZ1NUVnJGTFUvL25jNEtLRWZpNDZjT3VGOUU2NVRQb2lsamNIN28xT0VPMEdRPT0t"
    "LS9EWDlEUHJqMEtjc0RDUzlCMCthNXc5PQ%3D%3D--fecf5ac2f7b52cdf92111c2b559"
    "30278a04835e9"
)

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

TARGET_GAMES = {"2024 Summer Olympics", "2026 Winter Olympics"}

# Rate limiting
REQUEST_DELAY = 2.0
MAX_DELAY     = 60.0
MAX_RETRIES   = 6

# ── Columns matching the existing CSVs ────────────────────────────────────────
BIO_COLS = [
    "Roles", "Sex", "Full name", "Used name", "Born", "Died", "NOC",
    "athlete_id", "Measurements", "Affiliations", "Nick/petnames",
    "Title(s)", "Other names", "Nationality", "Original name", "Name order",
]

RESULTS_COLS = [
    "Games", "Event", "Team", "Pos", "Medal", "As",
    "athlete_id", "NOC", "Discipline", "Nationality",
]

# ── HTTP helpers ───────────────────────────────────────────────────────────────
_session = requests.Session()
_session.headers.update(HEADERS)

_current_delay = REQUEST_DELAY


def fetch(url: str) -> requests.Response | None:
    """GET with exponential back-off on 429."""
    global _current_delay
    delay = _current_delay
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(delay)
        try:
            resp = _session.get(url, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                _current_delay = REQUEST_DELAY
                return resp
            elif resp.status_code == 429:
                delay = min(delay * 2, MAX_DELAY)
                print(f"  [429] backing off {delay:.0f}s (attempt {attempt}/{MAX_RETRIES})")
            else:
                print(f"  [WARN] HTTP {resp.status_code} → {url}")
                return None
        except requests.RequestException as exc:
            print(f"  [WARN] request failed: {exc}")
            if attempt == MAX_RETRIES:
                return None
            delay = min(delay * 2, MAX_DELAY)
    return None


def get_soup(url: str) -> BeautifulSoup | None:
    resp = fetch(url)
    if resp is None:
        return None
    return BeautifulSoup(resp.content, "html.parser")


# ── Step 1: find edition IDs from /countries/USA ─────────────────────────────
def find_edition_ids(target_names: set[str]) -> dict[str, str]:
    """
    Scrape /countries/USA and return {game_name: edition_id} for target games.
    The 'Participations by edition' table has links like
    <a href="/editions/63">2024 Summer Olympics</a>.
    """
    print("Fetching /countries/USA to discover edition IDs …")
    page = get_soup(f"{BASE_URL}/countries/USA")
    if page is None:
        sys.exit("ERROR: could not fetch /countries/USA – check session cookie.")

    found: dict[str, str] = {}
    for a in page.find_all("a", href=True):
        m = re.match(r"^/editions/(\d+)$", a["href"])
        if m:
            text = a.get_text(strip=True)
            if text in target_names:
                found[text] = m.group(1)
                print(f"  Found: {text!r} → edition {m.group(1)}")

    missing = target_names - set(found.keys())
    if missing:
        print(f"  [WARN] could not find edition IDs for: {missing}")
    return found


# ── Step 2: collect US athlete IDs from /countries/USA/editions/{id} ────────
def get_us_athlete_ids(edition_id: str, game_name: str) -> list[str]:
    """
    Scrape /countries/USA/editions/{id}.
    The results table lists every US athlete as a link like
    <a href="/athletes/157252">Kyle Negomir</a>.
    Returns a de-duplicated list of athlete_id strings.
    """
    url = f"{BASE_URL}/countries/USA/editions/{edition_id}"
    print(f"  Fetching US athletes for {game_name} (edition {edition_id}) …")
    page = get_soup(url)
    if page is None:
        print(f"  [WARN] could not load {url}")
        return []

    ids: list[str] = []
    for a in page.find_all("a", href=True):
        m = re.match(r"^/athletes/(\d+)$", a["href"])
        if m:
            aid = m.group(1)
            if aid not in ids:
                ids.append(aid)

    print(f"    → {len(ids)} unique US athlete IDs")
    return ids


# ── Step 3: scrape bio for one athlete ────────────────────────────────────────
def scrape_bio(page: BeautifulSoup, athlete_id: str) -> dict:
    """
    Parse <table class="biodata"> on the athlete page.
    The table uses <th>key</th><td>value</td> rows, so we walk them directly
    with BeautifulSoup (handles flag images and anchor text cleanly).
    """
    bio: dict = {col: "" for col in BIO_COLS}
    bio["athlete_id"] = athlete_id

    table = page.find("table", {"class": "biodata"})
    if table is None:
        return bio

    for tr in table.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not (th and td):
            continue
        key = th.get_text(strip=True)
        val = td.get_text(separator=" ", strip=True)
        val = re.sub(r"\s{2,}", " ", val).strip()
        if key in bio:
            bio[key] = val

    return bio


# ── Step 4: scrape results for one athlete ────────────────────────────────────
_GAME_FILTER = re.compile(r"^(2024 Summer Olympics|2026 Winter Olympics)$")


def scrape_results(page: BeautifulSoup, athlete_id: str) -> list[dict]:
    """
    Parse <table class="table"> on the athlete page.

    Olympedia interleaves two row types:
      • Header rows  – Games cell filled; Discipline and NOC/Team hold sport
                       and country. Event/Pos/Medal cells are empty.
      • Event rows   – Games cell empty; Event, Pos, Medal cells are filled.

    We parse with BeautifulSoup first (strips flag images, <small> tags, etc.),
    then forward-fill Games/NOC/Discipline from header rows onto event rows,
    keeping only rows that match the two target games.
    """
    table = page.find("table", {"class": "table"})
    if table is None:
        return []

    # Walk rows manually to get clean text
    col_headers: list[str] = []
    cleaned_rows: list[list[str]] = []

    for tr in table.find_all("tr"):
        # thead
        if tr.parent.name == "thead":
            col_headers = [th.get_text(strip=True) for th in tr.find_all("th")]
            continue
        cells = []
        for td in tr.find_all(["td", "th"]):
            for small in td.find_all("small"):
                small.decompose()
            text = td.get_text(separator=" ", strip=True)
            text = re.sub(r"\s{2,}", " ", text).strip()
            cells.append(text)
        if cells:
            cleaned_rows.append(cells)

    if not col_headers or not cleaned_rows:
        return []

    # Normalise row length to header length
    n = len(col_headers)
    cleaned_rows = [r[:n] + [""] * (n - len(r)) for r in cleaned_rows]

    df = pd.DataFrame(cleaned_rows, columns=col_headers)

    # Drop the unnamed trailing column olympedia sometimes adds
    df.drop(columns=[""], inplace=True, errors="ignore")

    if "Games" not in df.columns:
        return []

    # Header rows have a non-empty Games cell; event rows have an empty one
    is_header = df["Games"] != ""
    is_event  = ~is_header

    noc_col  = "NOC / Team"
    disc_col = "Discipline (Sport) / Event"

    df["_noc"]  = ""
    df["_disc"] = ""

    if noc_col in df.columns:
        df.loc[is_header, "_noc"] = df.loc[is_header, noc_col]
    if disc_col in df.columns:
        df.loc[is_header, "_disc"] = df.loc[is_header, disc_col]

    # Forward-fill from header rows onto following event rows
    for col in ["Games", "_noc", "_disc", "As"]:
        if col in df.columns:
            df[col] = df[col].replace("", pd.NA).ffill().fillna("")

    # Keep only event rows that match the target games
    df = df[is_event].copy()
    df = df[df["Games"].apply(lambda g: bool(_GAME_FILTER.match(str(g))))]

    if df.empty:
        return []

    # For team events the event-row's NOC/Team cell holds a teammate name;
    # for individual events it's empty – either way that becomes Team.
    df["Team"]       = df[noc_col] if noc_col in df.columns else ""
    df["Event"]      = df[disc_col] if disc_col in df.columns else ""
    df["NOC"]        = df["_noc"]
    df["Discipline"] = df["_disc"]
    df["Nationality"] = ""

    pos_col   = "Pos"   if "Pos"   in df.columns else None
    medal_col = "Medal" if "Medal" in df.columns else None
    as_col    = "As"    if "As"    in df.columns else None

    out_rows: list[dict] = []
    for _, r in df.iterrows():
        row: dict = {col: "" for col in RESULTS_COLS}
        row["Games"]      = str(r.get("Games", "") or "")
        row["Event"]      = str(r.get("Event", "") or "")
        row["Team"]       = str(r.get("Team", "") or "")
        row["Pos"]        = str(r.get(pos_col, "") or "") if pos_col else ""
        row["Medal"]      = str(r.get(medal_col, "") or "") if medal_col else ""
        row["As"]         = str(r.get(as_col, "") or "") if as_col else ""
        row["athlete_id"] = athlete_id
        row["NOC"]        = str(r.get("NOC", "") or "")
        row["Discipline"] = str(r.get("Discipline", "") or "")
        row["Nationality"] = ""
        # Scrub any pandas NA string representations
        row = {k: "" if v in ("nan", "<NA>", "None") else v for k, v in row.items()}
        out_rows.append(row)

    return out_rows


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # 1. Find edition IDs via /countries/USA
    edition_ids = find_edition_ids(TARGET_GAMES)
    if not edition_ids:
        sys.exit(
            "ERROR: no target editions found.\n"
            "Check that the session cookie is valid and /countries/USA is reachable."
        )

    # 2. Collect all unique US athlete IDs across both editions
    all_athlete_ids: list[str] = []
    for game_name, eid in sorted(edition_ids.items()):
        for aid in get_us_athlete_ids(eid, game_name):
            if aid not in all_athlete_ids:
                all_athlete_ids.append(aid)

    total = len(all_athlete_ids)
    print(f"\nTotal unique US athletes to scrape: {total}")
    if total == 0:
        sys.exit(
            "No athlete IDs found. The page layout may have changed "
            "or the session cookie is expired."
        )

    # 3. Scrape bios + results for each athlete
    all_bios:    list[dict] = []
    all_results: list[dict] = []
    errors:      list[str]  = []

    for i, aid in enumerate(all_athlete_ids, 1):
        url = f"{BASE_URL}/athletes/{aid}"
        print(f"[{i}/{total}] athlete {aid} …", end=" ", flush=True)

        page = get_soup(url)
        if page is None:
            print("FAILED")
            errors.append(aid)
            continue

        bio = scrape_bio(page, aid)
        res = scrape_results(page, aid)
        all_bios.append(bio)
        all_results.extend(res)
        name = bio.get("Used name", "").replace("\u2022", " ").strip()
        print(f"ok  ({name})  {len(res)} result row(s)")

    # 4. Write outputs
    print(f"\nWriting {BIOS_OUT} …")
    with open(BIOS_OUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BIO_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_bios)

    print(f"Writing {RESULTS_OUT} …")
    with open(RESULTS_OUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_results)

    print(f"\n--- Done ---")
    print(f"Athletes scraped : {len(all_bios)}")
    print(f"Result rows      : {len(all_results)}")
    print(f"Errors           : {len(errors)}")
    if errors:
        print(f"Failed IDs: {errors}")


if __name__ == "__main__":
    main()
