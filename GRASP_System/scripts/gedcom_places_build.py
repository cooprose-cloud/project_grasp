#!/usr/bin/env python3
"""
gedcom_places_build.py
======================
Builds and maintains the Rose Family place name review file.

Each row represents one unique PLAC string from the GEDCOM. A human
reviewer classifies each place, fills in the structured database fields,
and sets a corrected place name. Downstream tools use this file to:
  - Replace PLAC values in the GEDCOM  (gedcom_places_apply.py)
  - Load places into MySQL standard or miscellaneous tables

STATUS CODES:
    U  Unreviewed    -- needs human attention (default for all new rows)
    S  Standard      -- confirmed 4-part place; goes to standard places
                        table in MySQL with all db_ fields populated
    M  Miscellaneous -- not decomposable to 4 parts; goes to misc places
                        table in MySQL; db_country should still be filled
    X  Exclude       -- junk, malformed, or to be dropped entirely

COLUMNS:
    gedcom_place      Raw place string exactly as it appears in the GEDCOM
    comma_count       Number of commas in gedcom_place (diagnostic)
    gedcom_count      Number of times this place appears in the GEDCOM
    status            U / S / M / X
    gedcom_corrected  What replaces gedcom_place in the cleaned GEDCOM.
                      Pre-filled for 3-comma places. Reviewer sets this
                      for all other entries before marking S or M.
    db_place          Full display name for MySQL (usually same as
                      gedcom_corrected). Pre-filled for 3-comma places.
    db_city           City component (pre-filled for 3-comma places)
    db_county         County component (pre-filled for 3-comma places)
    db_state          State / region component (pre-filled for 3-comma places)
    db_country        Country — the key sorting field in MySQL. Should be
                      filled even for M entries where other db_ fields
                      are blank (e.g. Italy, At Sea, Bermuda).
    notes             Free text for the reviewer

PRE-FILL RULES:
    3 commas  -> all fields pre-filled; status U (reviewer confirms -> S)
    other     -> gedcom_corrected, db_place, db_city, db_county, db_state
                 left blank; db_country filled if place is a known country;
                 reviewer fills missing fields before marking S or M

EXAMPLE — Franklin, Ohio, USA (2 commas, pre-filled partial):
    gedcom_place     = Franklin, Ohio, USA
    status           = U
    gedcom_corrected = (blank — reviewer fills: Franklin, Warren, Ohio, USA)
    db_place         = (blank — reviewer fills: Franklin, Warren, Ohio, USA)
    db_city          = Franklin
    db_county        = (blank — reviewer fills: Warren)
    db_state         = Ohio
    db_country       = USA
    -> Reviewer adds Warren County, sets gedcom_corrected and db_place,
       flips status to S. GEDCOM gets corrected to full 4-part form.

EXAMPLE — Italy (0 commas):
    gedcom_place     = Italy
    status           = U
    gedcom_corrected = (blank — reviewer fills: Italy)
    db_place         = (blank — reviewer fills: Italy)
    db_city          = (blank)
    db_county        = (blank)
    db_state         = (blank)
    db_country       = Italy
    -> Reviewer sets gedcom_corrected=Italy, db_place=Italy, status=M.
       GEDCOM keeps Italy; MySQL has country=Italy for sorting.

COMMANDS:
    build          Build or refresh place file from a GEDCOM.
                   Carries forward all prior reviewed rows unchanged.

    import-legacy  Convert a gedcom_places.py-style CSV into the new
                   format, then merge with a fresh GEDCOM scan.

Usage:
    # First build (no prior file):
    python gedcom_places_build.py build \\
        --input  file.ged \\
        --output places.csv

    # Refresh after new GEDCOM export:
    python gedcom_places_build.py build \\
        --input    file.ged \\
        --existing places.csv \\
        --output   places_new.csv

    # Migrate from old gedcom_places.py output:
    python gedcom_places_build.py import-legacy \\
        --input   file.ged \\
        --legacy  old_places.csv \\
        --output  places.csv

#    python scripts/gedcom_places_build.py build --input Final_Build_667_1.ged --output places1.csv

#   python scripts/gedcom_places_build.py build --input out5_667.ged --output places1.csv

#   python scripts/gedcom_places_build.py build --input out5_667.ged --output places1.csv


#   python scripts/gedcom_places_build.py build --input out5_667.ged --output places1.csv

#   
#   
#    python scripts/gedcom_places_build.py build --input out5_667.ged --existing places1.csv --output places1.ged

#    python scripts/gedcom_places_build.py import-legacy --input places1.ged --legacy  places2.csv --output  places2.ged

    python gedcom_places_build.py import-legacy --input   Final_Build_667_1.ged --legacy  Places1.csv --output  places2.csv    


    python scripts/gedcom_places_build.py build --input Final_Build_667_1.ged --output places1.csv
    python scripts/gedcom_places_build.py import-legacy --input   Final_Build_667_1.ged --legacy  Places1.csv --output  places2.csv 




"""

import argparse
import csv
import os
import re
import sys
from collections import Counter


# ---------------------------------------------------------------------------
# COUNTRY NORMALIZATION
# ---------------------------------------------------------------------------

COUNTRY_NORM = {
    "usa":                       "USA",
    "u.s.a.":                    "USA",
    "u.s.a":                     "USA",
    "us":                        "USA",
    "united states":             "USA",
    "united states of america":  "USA",
    "america":                   "USA",
    "england":                   "England",
    "scotland":                  "Scotland",
    "wales":                     "Wales",
    "uk":                        "UK",
    "u.k.":                      "UK",
    "united kingdom":            "UK",
    "great britain":             "UK",
    "britain":                   "UK",
    "germany":                   "Germany",
    "deutschland":               "Germany",
    "german empire":             "Germany",
    "france":                    "France",
    "ireland":                   "Ireland",
    "eire":                      "Ireland",
    "italy":                     "Italy",
    "italia":                    "Italy",
    "canada":                    "Canada",
    "australia":                 "Australia",
    "new zealand":               "New Zealand",
    "austria":                   "Austria",
    "sweden":                    "Sweden",
    "norway":                    "Norway",
    "denmark":                   "Denmark",
    "netherlands":               "Netherlands",
    "holland":                   "Netherlands",
    "switzerland":               "Switzerland",
    "bermuda":                   "Bermuda",
    "panama":                    "Panama",
    "lebanon":                   "Lebanon",
}


def normalize_country(raw):
    """Return canonical country name if raw matches a known variant."""
    return COUNTRY_NORM.get(raw.strip().lower(), raw.strip())


def is_known_country(raw):
    """Return True if raw is a recognized country name or variant."""
    return raw.strip().lower() in COUNTRY_NORM


# ---------------------------------------------------------------------------
# US STATE DETECTION (for partial pre-fill of 1 and 2-comma places)
# ---------------------------------------------------------------------------

US_STATES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District of Columbia",
}


# ---------------------------------------------------------------------------
# GEDCOM PARSING
# ---------------------------------------------------------------------------

def read_gedcom_lines(filepath):
    results = []
    with open(filepath, encoding="utf-8-sig", errors="replace") as fh:
        for raw in fh:
            raw = raw.rstrip("\r\n")
            parts = raw.strip().split(" ", 2)
            if len(parts) < 2:
                results.append((None, None, None, raw))
                continue
            try:
                level = int(parts[0])
            except ValueError:
                results.append((None, None, None, raw))
                continue
            tag  = parts[1] if len(parts) > 1 else ""
            rest = parts[2] if len(parts) > 2 else ""
            results.append((level, tag, rest, raw))
    return results


def collect_place_counts(gedcom_lines):
    """Return Counter of {place_string: count} for all PLAC tags."""
    counter = Counter()
    n = len(gedcom_lines)
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if tag == "PLAC" and rest:
            val = rest.strip()
            j = i + 1
            while j < n:
                lv, tg, rt, _ = gedcom_lines[j]
                if lv is not None and lv > level and tg in ("CONC", "CONT"):
                    val += rt
                    j += 1
                else:
                    break
            counter[val.strip()] += 1
            i = j
        else:
            i += 1
    return counter


# ---------------------------------------------------------------------------
# PRE-FILL LOGIC
# ---------------------------------------------------------------------------

def prefill_row(gedcom_place, gedcom_count):
    """
    Build a new unreviewed row for a place string.

    3-comma places: all fields pre-filled, reviewer just confirms.
    Other places:   partial pre-fill where possible; reviewer completes.
    """
    comma_count = gedcom_place.count(",")
    parts = [p.strip() for p in gedcom_place.split(",")]

    gedcom_corrected = ""
    db_place         = ""
    db_city          = ""
    db_county        = ""
    db_state         = ""
    db_country       = ""

    if comma_count == 3:
        # Ideal 4-part place — pre-fill everything
        db_city    = parts[0]
        db_county  = parts[1]
        db_state   = parts[2]
        db_country = normalize_country(parts[3])
        gedcom_corrected = f"{db_city}, {db_county}, {db_state}, {db_country}"
        db_place         = gedcom_corrected

    elif comma_count == 2:
        # 3-part: City, Region, Country  or  City, County, State
        db_city = parts[0]
        if is_known_country(parts[2]):
            db_state   = parts[1]
            db_country = normalize_country(parts[2])
        elif parts[2] in US_STATES:
            db_state   = parts[2]
            db_country = "USA"
        else:
            # Unknown pattern — fill what we can
            db_state   = parts[2]
            db_country = normalize_country(parts[2]) if is_known_country(parts[2]) else ""

    elif comma_count == 1:
        # 2-part: State/Region, Country  or  City, Country
        if is_known_country(parts[1]):
            db_country = normalize_country(parts[1])
            if parts[0] in US_STATES:
                db_state = parts[0]
            else:
                db_city = parts[0]
        elif parts[1] in US_STATES:
            db_city    = parts[0]
            db_state   = parts[1]
            db_country = "USA"
        else:
            db_city    = parts[0]
            db_state   = parts[1]

    elif comma_count == 0:
        # Single value — country, state, or miscellaneous
        if is_known_country(gedcom_place):
            db_country = normalize_country(gedcom_place)
        elif gedcom_place in US_STATES:
            db_state   = gedcom_place
            db_country = "USA"
        # else: junk / address — leave all blank

    return {
        "gedcom_place":      gedcom_place,
        "comma_count":       comma_count,
        "gedcom_count":      gedcom_count,
        "status":            "U",
        "gedcom_corrected":  gedcom_corrected,
        "db_place":          db_place,
        "db_city":           db_city,
        "db_county":         db_county,
        "db_state":          db_state,
        "db_country":        db_country,
        "notes":             "",
    }


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "gedcom_place",
    "comma_count",
    "gedcom_count",
    "status",
    "gedcom_corrected",
    "db_place",
    "db_city",
    "db_county",
    "db_state",
    "db_country",
    "notes",
]

STATUS_ORDER = {"U": 0, "S": 1, "M": 2, "X": 3}


def load_existing_place_file(csv_path):
    """Load existing place file into dict keyed on gedcom_place."""
    rows = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            for fn in FIELDNAMES:
                if fn not in row:
                    row[fn] = ""
            rows[row["gedcom_place"]] = dict(row)
    return rows


def write_place_file(rows, csv_path):
    """
    Write rows to CSV sorted by:
      1. Status (U first so unreviewed work is at top)
      2. Descending gedcom_count (high-frequency places first)
      3. gedcom_place alphabetically
    """
    sorted_rows = sorted(
        rows,
        key=lambda r: (
            STATUS_ORDER.get(r.get("status", "U"), 9),
            -int(r.get("gedcom_count", 0) or 0),
            r.get("gedcom_place", ""),
        )
    )
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted_rows)
    return sorted_rows


# ---------------------------------------------------------------------------
# LEGACY IMPORT — old gedcom_places.py format
# ---------------------------------------------------------------------------
# Old columns: Original Place, Count, Corrected Place, City, County,
#              State, Country, Event Types, Num Individuals
# No status column existed — everything imports as U (unreviewed).
# 4-part places with all fields populated get gedcom_corrected and
# db_place pre-filled. Incomplete places get partial db_ fields only.

def import_legacy_row(old_row):
    """Convert a row from the old gedcom_places.py CSV to the new format."""
    def get(row, *keys):
        for k in keys:
            if k in row and row[k].strip():
                return row[k].strip()
        return ""

    gedcom_place = get(old_row, "Original Place")
    city         = get(old_row, "City")
    county       = get(old_row, "County")
    state        = get(old_row, "State")
    country_raw  = get(old_row, "Country")
    country      = normalize_country(country_raw) if country_raw else ""
    comma_count  = gedcom_place.count(",")

    # Only pre-fill gedcom_corrected / db_place when all 4 fields present
    if city and county and state and country:
        gedcom_corrected = f"{city}, {county}, {state}, {country}"
        db_place         = gedcom_corrected
    else:
        gedcom_corrected = ""
        db_place         = ""

    return {
        "gedcom_place":     gedcom_place,
        "comma_count":      comma_count,
        "gedcom_count":     0,   # updated from GEDCOM scan below
        "status":           "U",
        "gedcom_corrected": gedcom_corrected,
        "db_place":         db_place,
        "db_city":          city,
        "db_county":        county,
        "db_state":         state,
        "db_country":       country,
        "notes":            "",
    }


# ---------------------------------------------------------------------------
# BUILD COMMAND
# ---------------------------------------------------------------------------

def cmd_build(args):
    quiet = getattr(args, 'quiet', False)
    if not quiet:
        print(f"\n=== BUILD PLACE FILE ===")
        print(f"  GEDCOM  : {args.input}")
        if args.existing:
            print(f"  Existing: {args.existing}")
        print(f"  Output  : {args.output}")

    if not os.path.isfile(args.input):
        print(f"ERROR: GEDCOM not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if not quiet:
        print("  Parsing GEDCOM...")
    gedcom_lines = read_gedcom_lines(args.input)
    place_counts = collect_place_counts(gedcom_lines)
    if not quiet:
        print(f"  Found {len(place_counts)} unique place strings "
              f"({sum(place_counts.values())} total references)")

    existing = {}
    if args.existing:
        if not os.path.isfile(args.existing):
            print(f"ERROR: Existing file not found: {args.existing}", file=sys.stderr)
            sys.exit(1)
        existing = load_existing_place_file(args.existing)
        if not quiet:
            print(f"  Loaded {len(existing)} rows from existing place file")

    carried_forward = 0
    newly_added     = 0
    output_rows     = []

    for gedcom_place, count in place_counts.items():
        if gedcom_place in existing:
            row = dict(existing[gedcom_place])
            row["gedcom_count"] = count
            output_rows.append(row)
            carried_forward += 1
        else:
            output_rows.append(prefill_row(gedcom_place, count))
            newly_added += 1

    # Rows in existing file no longer in GEDCOM
    obsolete = 0
    if existing:
        for place, row in existing.items():
            if place not in place_counts:
                obsolete += 1
                if not args.drop_obsolete:
                    row = dict(row)
                    row["gedcom_count"] = 0
                    row["notes"] = (
                        (row.get("notes", "") + " [NOT IN CURRENT GEDCOM]").strip()
                    )
                    output_rows.append(row)

    write_place_file(output_rows, args.output)

    u = sum(1 for r in output_rows if r["status"] == "U")
    s = sum(1 for r in output_rows if r["status"] == "S")
    m = sum(1 for r in output_rows if r["status"] == "M")
    x = sum(1 for r in output_rows if r["status"] == "X")

    if not quiet:
        print(f"\n  Output rows        : {len(output_rows)}")
        print(f"    Carried forward  : {carried_forward}")
        print(f"    Newly added      : {newly_added}")
        if existing:
            print(f"    Obsolete         : {obsolete}")
        print(f"\n  Status breakdown:")
        print(f"    U (Unreviewed)   : {u}")
        print(f"    S (Standard)     : {s}")
        print(f"    M (Miscellaneous): {m}")
        print(f"    X (Exclude)      : {x}")

        # Review workload summary
        new_u = [r for r in output_rows if r["status"] == "U"]
        prefilled  = sum(1 for r in new_u if r["gedcom_corrected"])
        needs_work = sum(1 for r in new_u if not r["gedcom_corrected"])
        print(f"\n  Of {len(new_u)} unreviewed rows:")
        print(f"    {prefilled}  pre-filled (just need confirmation -> S or M)")
        print(f"    {needs_work}  need manual entry before marking S or M")
        print(f"\n  Place file written: {args.output}")


# ---------------------------------------------------------------------------
# IMPORT-LEGACY COMMAND
# ---------------------------------------------------------------------------

def cmd_import_legacy(args):
    quiet = getattr(args, 'quiet', False)
    if not quiet:
        print(f"\n=== IMPORT LEGACY PLACE FILE ===")
        print(f"  GEDCOM : {args.input}")
        print(f"  Legacy : {args.legacy}")
        print(f"  Output : {args.output}")

    for f in [args.input, args.legacy]:
        if not os.path.isfile(f):
            print(f"ERROR: File not found: {f}", file=sys.stderr)
            sys.exit(1)

    if not quiet:
        print("  Parsing GEDCOM...")
    gedcom_lines = read_gedcom_lines(args.input)
    place_counts = collect_place_counts(gedcom_lines)
    if not quiet:
        print(f"  Found {len(place_counts)} unique place strings")

    legacy_rows = {}
    with open(args.legacy, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            converted = import_legacy_row(row)
            if converted["gedcom_place"]:
                legacy_rows[converted["gedcom_place"]] = converted

    # Update counts from GEDCOM
    for place, count in place_counts.items():
        if place in legacy_rows:
            legacy_rows[place]["gedcom_count"] = count

    # Merge: legacy rows + any new places not in legacy
    output_rows = list(legacy_rows.values())
    added = 0
    for place, count in place_counts.items():
        if place not in legacy_rows:
            output_rows.append(prefill_row(place, count))
            added += 1

    write_place_file(output_rows, args.output)

    if not quiet:
        prefilled = sum(1 for r in output_rows if r["gedcom_corrected"])
        print(f"\n  Legacy rows converted : {len(legacy_rows)}")
        print(f"  New rows added        : {added}")
        print(f"  Total output rows     : {len(output_rows)}")
        print(f"    Pre-filled          : {prefilled}")
        print(f"    Need manual entry   : {len(output_rows) - prefilled}")
        print(f"\n  Output written: {args.output}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build and maintain the Rose Family place name review file."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build",
                              help="Build or refresh place file from GEDCOM")
    p_build.add_argument("--input",        required=True,
                         help="Input GEDCOM file (.ged)")
    p_build.add_argument("--output",       required=True,
                         help="Output place CSV file")
    p_build.add_argument("--existing",     default=None,
                         help="Existing place file to carry forward (optional)")
    p_build.add_argument("--drop-obsolete", action="store_true",
                         help="Drop rows no longer in GEDCOM "
                              "(default: keep with note)")
    p_build.add_argument("--quiet", action="store_true",
                         help="Suppress progress output; errors go to stderr")

    p_legacy = sub.add_parser("import-legacy",
                               help="Convert old gedcom_places.py format")
    p_legacy.add_argument("--input",  required=True,
                          help="Input GEDCOM file (.ged)")
    p_legacy.add_argument("--legacy", required=True,
                          help="Legacy place CSV (gedcom_places.py format)")
    p_legacy.add_argument("--output", required=True,
                          help="Output place CSV file")
    p_legacy.add_argument("--quiet", action="store_true",
                          help="Suppress progress output; errors go to stderr")

    args = parser.parse_args()
    if args.command == "build":
        cmd_build(args)
    elif args.command == "import-legacy":
        cmd_import_legacy(args)


if __name__ == "__main__":
    main()
