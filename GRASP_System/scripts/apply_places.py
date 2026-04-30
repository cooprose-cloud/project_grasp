"""
apply_places.py  —  Read a places CSV and update the places table db_* columns.
Matches on place_name (gedcom_corrected field) against places.place_name.

Usage:
    python3 apply_places.py --config /path/to/config.ini --csv /path/to/places.csv
"""
import csv
import mysql.connector
import sys
import argparse
import configparser
import os


def load_config(config_path):
    """Load database settings from config.ini."""
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    config.read(config_path)
    return dict(
        host     = config.get('Database', 'Host',     fallback='localhost'),
        user     = config.get('Database', 'User',     fallback='root'),
        password = config.get('Database', 'Password', fallback=''),
        database = config.get('Database', 'Database', fallback='genealogy'),
    )


def main():
    parser = argparse.ArgumentParser(description="Apply places CSV to MySQL places table.")
    parser.add_argument("--config", required=True,
                        help="Path to config.ini (contains database credentials)")
    parser.add_argument("--csv", required=True,
                        help="Path to places CSV file (e.g. GRASP_User/csv/places2.csv)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress progress output; errors go to stderr")
    args = parser.parse_args()
    quiet = args.quiet

    DB = load_config(args.config)
    CSV_FILE = args.csv

    if not os.path.exists(CSV_FILE):
        print(f"ERROR: CSV file not found: {CSV_FILE}", file=sys.stderr)
        sys.exit(1)

    conn = mysql.connector.connect(**DB)
    cursor = conn.cursor(dictionary=True)

    # Load all current place_names from DB
    cursor.execute("SELECT place_id, place_name FROM places")
    db_places = {row['place_name']: row['place_id'] for row in cursor.fetchall()}
    if not quiet:
        print(f"Loaded {len(db_places)} places from database.")

    updated = 0
    skipped = 0
    not_found = []

    with open(CSV_FILE, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use gedcom_corrected if present, otherwise fall back to gedcom_place
            place_name = (row['gedcom_corrected'].strip()
                          or row['gedcom_place'].strip())
            db_city    = row['db_city'].strip()
            db_county  = row['db_county'].strip()
            db_state   = row['db_state'].strip()
            db_country = row['db_country'].strip()

            if place_name not in db_places:
                not_found.append(place_name)
                skipped += 1
                continue

            cursor.execute("""
                UPDATE places
                SET db_city=%s, db_county=%s, db_state=%s, db_country=%s
                WHERE place_name=%s
            """, (db_city or None, db_county or None, db_state or None, db_country or None, place_name))
            updated += 1

    conn.commit()
    if not quiet:
        print(f"Updated: {updated}  Skipped (not in DB): {skipped}")
    if not_found:
        print(f"WARNING: {len(not_found)} place(s) not found in DB:", file=sys.stderr)
        for n in not_found:
            print(f"  '{n}'", file=sys.stderr)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
