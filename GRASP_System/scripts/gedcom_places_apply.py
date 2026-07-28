




#!/usr/bin/env python3
"""
gedcom_places_apply.py

Applies corrected place names from a reviewed places CSV file back to a GEDCOM file.

Usage:
    python3 gedcom_places_apply.py input.ged places.csv
    python3 gedcom_places_apply.py input.ged places.csv --output output.ged
    python3 gedcom_places_apply.py input.ged places.csv --dry-run

The places CSV is produced by gedcom_places_build.py and reviewed by the user.

Status codes in the CSV:
    S = Standard corrected — apply gedcom_corrected value to the GEDCOM
    M = Non-conformist but intentionally correct — leave as-is
    U = Unreviewed — skip (no change)

Only rows where status = 'S' AND gedcom_corrected differs from gedcom_place are applied.
"""

import sys
import os
import csv
import argparse
from datetime import datetime


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_place_corrections(csv_path):
    """
    Load the places CSV and build a dict mapping original gedcom_place ->
    gedcom_corrected for all rows where status = 'S' and the value changed.

    Returns:
        corrections  dict  { original_place: corrected_place }
        skipped_m    int   count of M-status rows (intentionally left alone)
        skipped_u    int   count of U-status rows (unreviewed)
        no_change    int   count of S-status rows where value didn't change
    """
    corrections = {}
    skipped_m = 0
    skipped_u = 0
    no_change = 0
    defaulted = 0
    errors = []

    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        # Normalize header names (strip whitespace, lowercase for lookup)
        fieldnames = reader.fieldnames
        if fieldnames is None:
            print(f"ERROR: CSV file appears to be empty: {csv_path}")
            sys.exit(1)

        # Build a case-insensitive column map
        col_map = {name.strip().lower(): name for name in fieldnames}

        def get_col(row, *candidates):
            """Return value for the first matching column name."""
            for c in candidates:
                if c in col_map:
                    return row.get(col_map[c], '').strip()
            return ''

        for i, row in enumerate(reader, start=2):  # row 1 = header
            status       = get_col(row, 'status').upper()
            gedcom_place = get_col(row, 'gedcom_place', 'original_place')
            gedcom_corr  = get_col(row, 'gedcom_corrected', 'corrected_place')

            if not gedcom_place:
                continue  # blank row

            if status == 'U':
                skipped_u += 1
                continue

            if status == 'M':
                skipped_m += 1
                continue

            if status == 'S':
                if not gedcom_corr:
                    # Default to gedcom_place — place is already correct as-is
                    gedcom_corr = gedcom_place
                    defaulted += 1
                if gedcom_corr == gedcom_place:
                    no_change += 1
                    continue
                corrections[gedcom_place] = gedcom_corr
            else:
                # Unknown status — skip silently
                pass

    if errors:
        print("WARNINGS — rows skipped due to data issues:")
        for e in errors:
            print(e)
        print()

    return corrections, skipped_m, skipped_u, no_change, defaulted


# ---------------------------------------------------------------------------
# GEDCOM processing
# ---------------------------------------------------------------------------

def get_output_filename(input_path):
    """Generate default output filename by inserting '_placefixed' before extension."""
    base, ext = os.path.splitext(input_path)
    return f"{base}_placefixed{ext}"


def apply_corrections(input_path, output_path, corrections, dry_run=False):
    """
    Read the GEDCOM, replace PLAC values according to the corrections dict,
    and write the result to output_path (unless dry_run=True).

    Returns:
        changed_count   int  number of PLAC lines actually changed
        unchanged_count int  number of PLAC lines left untouched
        not_found       set  place strings found in GEDCOM but not in corrections
    """
    with open(input_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        lines = f.readlines()

    out_lines = []
    changed_count   = 0
    unchanged_count = 0
    not_found       = set()
    changed_places  = {}  # original -> corrected, for the summary

    for line in lines:
        stripped = line.rstrip('\n\r')

        # Match any level PLAC tag:  "N PLAC <value>"
        # We look for the pattern rather than hard-coding level 2 because
        # PLAC can appear at level 2 (under events) or elsewhere.
        parts = stripped.split(' ', 2)
        if len(parts) >= 3 and parts[1].upper() == 'PLAC':
            level      = parts[0]
            tag        = parts[1]
            plac_value = parts[2].strip()

            if plac_value in corrections:
                corrected = corrections[plac_value]
                new_line  = f"{level} {tag} {corrected}\n"
                out_lines.append(new_line)
                changed_count += 1
                changed_places[plac_value] = corrected
            else:
                # Not in corrections dict — leave it alone
                out_lines.append(line)
                unchanged_count += 1
                # Track if this is a place we'd expect to find (not empty)
                if plac_value:
                    not_found.add(plac_value)
        else:
            out_lines.append(line)

    if not dry_run:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.writelines(out_lines)

    return changed_count, unchanged_count, not_found, changed_places


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(csv_path, input_path, output_path,
                  corrections, skipped_m, skipped_u, no_change, defaulted,
                  changed_count, unchanged_count, not_found, changed_places,
                  dry_run):
    """Print a clear summary of what was done."""

    print("=" * 125)
    print("  gedcom_places_apply.py — Place Name Correction Summary")
    print("=" * 125)
    print(f"  Run date  : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  GEDCOM in : {input_path}")
    if dry_run:
        print(f"  Output    : (dry run — no file written)")
    else:
        print(f"  GEDCOM out: {output_path}")
    print(f"  CSV file  : {csv_path}")
    print()

    print("CSV File Statistics:")
    print(f"  Corrections to apply (status=S, value changed) : {len(corrections):>5}")
    print(f"  Confirmed correct, no change (status=S, same)  : {no_change:>5}")
    print(f"  Confirmed correct, corrected defaulted to orig : {defaulted:>5}")
    print(f"  Skipped — non-conformist correct (status=M)    : {skipped_m:>5}")
    print(f"  Skipped — unreviewed (status=U)                : {skipped_u:>5}")
    print()

    print("GEDCOM Processing Results:")
    print(f"  PLAC lines updated                             : {changed_count:>5}")
    print(f"  PLAC lines left unchanged                      : {unchanged_count:>5}")
    print()

    # Report any PLAC values in the GEDCOM that weren't in the corrections dict
    # (filter out the ones that were M/U/no-change — they're expected to be missing)
    # We can't easily know which bucket they fell into, so just report count.
    total_plac = changed_count + unchanged_count
    print(f"  Total PLAC lines in GEDCOM                     : {total_plac:>5}")
    print()

    if changed_places:
        print(f"Places Updated ({len(changed_places)} unique values):")
        print(f"  {'Original':<60}  {'Corrected'}")
        print(f"  {'-'*60}  {'-'*55}")
        for orig, corr in sorted(changed_places.items()):
            # Truncate long strings for display
            orig_disp = orig[:59] if len(orig) > 59 else orig
            corr_disp = corr[:54] if len(corr) > 54 else corr
            print(f"  {orig_disp:<60}  {corr_disp}")
        print()

    if dry_run:
        print("DRY RUN — no output file was written.")
    else:
        print(f"Output written to: {output_path}")

    print("=" * 125)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Apply corrected place names from a CSV file to a GEDCOM file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 gedcom_places_apply.py xROSv.ged places.csv
  python3 gedcom_places_apply.py xROSv.ged places.csv --output xROSv_fixed.ged
  python3 gedcom_places_apply.py xROSv.ged places.csv --dry-run
        """
    )
    parser.add_argument('gedcom',  help='Input GEDCOM file (.ged)')
    parser.add_argument('csv',     help='Places CSV file produced by gedcom_places_build.py')
    parser.add_argument('--output', '-o',
                        help='Output GEDCOM file (default: input_placefixed.ged)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be changed without writing output file')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress progress output; errors go to stderr')

    args = parser.parse_args()

    # Validate inputs
    if not os.path.exists(args.gedcom):
        print(f"ERROR: GEDCOM file not found: {args.gedcom}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV file not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    output_path = args.output or get_output_filename(args.gedcom)

    if not args.dry_run and os.path.abspath(output_path) == os.path.abspath(args.gedcom):
        print("ERROR: Output file cannot be the same as the input file.", file=sys.stderr)
        print(f"  Use --output to specify a different filename.", file=sys.stderr)
        sys.exit(1)

    if not args.quiet:
        print(f"Loading corrections from: {args.csv}")
    corrections, skipped_m, skipped_u, no_change, defaulted = load_place_corrections(args.csv)
    if not args.quiet:
        print(f"  {len(corrections)} place corrections ready to apply.")

    if not corrections:
        if not args.quiet:
            print("\nNothing to apply — no status=S rows with changed values found.")
            print("Have you reviewed and updated the CSV yet?")
        sys.exit(0)

    if not args.quiet:
        print(f"\nProcessing GEDCOM: {args.gedcom}")
    changed_count, unchanged_count, not_found, changed_places = apply_corrections(
        args.gedcom, output_path, corrections, dry_run=args.dry_run
    )

    if not args.quiet:
        print_summary(
            args.csv, args.gedcom, output_path,
            corrections, skipped_m, skipped_u, no_change, defaulted,
            changed_count, unchanged_count, not_found, changed_places,
            args.dry_run
        )


if __name__ == '__main__':
    main()
