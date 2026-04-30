#!/usr/bin/env python3
"""

#####.    python3 scripts/gedcom_cleanup.py detect --input /users/jamesrose/coops_test/coops_in/gedcom/Liz_Test.ged --report_citations /users/jamesrose/coops_test/coops_in/citations.csv --report_publ /users/jamesrose/coops_test/coops_in/publ.csv --report_resi_dates /users/jamesrose/coops_test/coops_in/resi.csv --report_media /users/jamesrose/coops_test/coops_in/media.csv --report_mojibake /users/jamesrose/coops_test/coops_in/mojibake.csv --report_dates /users/jamesrose/coops_test/coops_in/scripts/resi_dates.csv --report_photo /users/jamesrose/coops_test/coops_in/photo.csv 
#####.    python3 scripts/gedcom_cleanup.py apply --input /users/jamesrose/coops_test/coops_in/gedcom/Liz_Test.ged --output /users/jamesrose/coops_test/coops_in/gedcom/Liz_Test1.ged --report_citations /users/jamesrose/coops_test/coops_in/citations.csv --report_publ /users/jamesrose/coops_test/coops_in/publ.csv --report_resi_dates /users/jamesrose/coops_test/coops_in/resi.csv --report_media /users/jamesrose/coops_test/coops_in/media.csv --report_mojibake /users/jamesrose/coops_test/coops_in/mojibake.csv --report_dates /users/jamesrose/coops_test/coops_in/resi_dates.csv


gedcom_cleanup.py
=================
Consolidated GEDCOM cleanup tool combining four cleanup operations:

    citations   -- Detect and consolidate duplicate SOUR citation blocks
    publ        -- Clean corrupted PUBL (publisher) tags from Ancestry exports
    resi_dates  -- Infer missing DATE values for RESI events from source citations
    media       -- Detect and remove duplicate OBJE (media) attachments

Each tool follows the same two-phase detect/apply pattern:
    detect  -- Scan GEDCOM, write CSV report for review
    apply   -- Read reviewed CSV, write cleaned GEDCOM

Usage:
    python gedcom_cleanup.py <tool> detect --input file.ged --report report.csv
    python gedcom_cleanup.py <tool> apply  --input file.ged --report report.csv --output out.ged


    python scripts/gedcom_cleanup.py citations detect  --input Liz_Test.ged --report_citations citations_667.csv
    python scripts/gedcom_cleanup.py citations apply  --input Final_Build_667.ged --report Final_Report_667_report.csv


    python3 /users/jamesrose/Test_Data/gedcom_cleanup.py detect --input Grasp_Final.ged

    # Detect — run any combination of tools in one pass
    # Apply — run any combination of tools in one pass
    python gedcom_cleanup.py apply --input Final_Build_667.ged --output Final_Gedcom_667_1.ged --report_citations citations_667.csv --report_publ publ_667.csv --report_resi_dates resi_667.csv --report_media media_667.csv


Tools:
    citations   detect --input file.ged --report citations.csv
    citations   apply  --input file.ged --report citations.csv --output out.ged

    publ        detect --input file.ged --report publ.csv
    publ        apply  --input file.ged --report publ.csv --output out.ged

    resi_dates  detect --input file.ged --report resi_dates.csv
    resi_dates  apply  --input file.ged --report resi_dates.csv --output out.ged

    media       detect --input file.ged --report media.csv
    media       apply  --input file.ged --report media.csv --output out.ged

"""

import argparse
import configparser
import csv
import os
import re
import shutil
import sys
import tempfile
from collections import defaultdict

# Module-level quiet flag — set to True by --quiet at startup
_quiet = False


def _print(*args, **kwargs):
    if not _quiet:
        print(*args, **kwargs)



# ===========================================================================
# SHARED GEDCOM UTILITIES
# ===========================================================================

def read_gedcom_lines(filepath):
    """Return list of (level, tag, rest, raw_line) for every line."""
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


def is_pointer(value):
    """Return True if value looks like a GEDCOM pointer e.g. @I1@."""
    return value.startswith("@") and value.endswith("@")


def get_block_lines(gedcom_lines, start_idx):
    """
    Return list of line indices forming the block starting at start_idx.
    Includes start_idx and all subsequent lines whose level > start level.
    """
    level = gedcom_lines[start_idx][0]
    indices = [start_idx]
    j = start_idx + 1
    n = len(gedcom_lines)
    while j < n:
        lv = gedcom_lines[j][0]
        if lv is None or lv <= level:
            break
        indices.append(j)
        j += 1
    return indices


def require_files(*paths):
    """Exit with error if any file path does not exist."""
    for p in paths:
        if not os.path.isfile(p):
            sys.exit(f"ERROR: File not found: {p}")


def require_distinct(input_path, output_path):
    """Exit with error if input and output paths are the same file."""
    if os.path.abspath(input_path) == os.path.abspath(output_path):
        sys.exit("ERROR: Output file must differ from input file.")


# Shared event tag set used by citations and media tools
EVENT_TAGS = {
    "BIRT", "CHR", "DEAT", "BURI", "CREM", "ADOP",
    "BAPM", "BARM", "BASM", "BLES", "CHRA", "CONF",
    "FCOM", "ORDN", "NATU", "EMIG", "IMMI", "CENS",
    "PROB", "WILL", "GRAD", "RETI", "EVEN",
    "MARR", "ENGA", "MARB", "MARC", "MARL", "MARS",
    "DIVF", "DIV", "ANUL",
    "RESI", "OCCU", "EDUC", "RELI", "NAME",
}

TOP_RECORDS = {"INDI", "FAM", "SOUR"}


# ===========================================================================
# TOOL: CITATIONS
# Detects and consolidates duplicate SOUR citation blocks within the same
# record and event context.
# ===========================================================================

def _citations_block_child_count(gedcom_lines, start_idx):
    return len(get_block_lines(gedcom_lines, start_idx)) - 1


def _citations_block_page_value(gedcom_lines, start_idx):
    base_level = gedcom_lines[start_idx][0]
    j = start_idx + 1
    n = len(gedcom_lines)
    while j < n:
        lv, tg, rest, _ = gedcom_lines[j]
        if lv is None:
            j += 1
            continue
        if lv <= base_level:
            break
        if lv == base_level + 1 and tg == "PAGE":
            return rest
        j += 1
    return ""


def citations_detect(gedcom_lines):
    """
    Walk GEDCOM and find duplicate SOUR citations within the same
    record + event context. Returns list of report row dicts.
    """
    occurrences = defaultdict(list)

    current_rec_type  = None
    current_rec_id    = None
    current_event_tag = None
    current_event_lvl = None

    n = len(gedcom_lines)
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]

        if level is None:
            i += 1
            continue

        if level == 0:
            current_rec_type  = None
            current_rec_id    = None
            current_event_tag = None
            current_event_lvl = None
            if tag and is_pointer(tag) and rest in TOP_RECORDS:
                current_rec_type = rest
                current_rec_id   = tag
            i += 1
            continue

        if current_rec_type is None:
            i += 1
            continue

        if level == 1 and tag in EVENT_TAGS:
            current_event_tag = tag
            current_event_lvl = level
        elif current_event_lvl is not None and level <= current_event_lvl:
            if tag not in EVENT_TAGS:
                current_event_tag = None
                current_event_lvl = None

        if tag == "SOUR" and is_pointer(rest):
            ctx = current_event_tag if current_event_tag else "(top-level)"
            key = (current_rec_type, current_rec_id, ctx, rest)
            occurrences[key].append(i)

        i += 1

    report_rows = []
    for key, line_indices in occurrences.items():
        if len(line_indices) < 2:
            continue

        rec_type, rec_id, ctx, sour_ref = key
        scored = []
        for idx in line_indices:
            children = _citations_block_child_count(gedcom_lines, idx)
            page     = _citations_block_page_value(gedcom_lines, idx)
            scored.append((idx, children, page))

        best_idx = max(scored, key=lambda x: (x[1], len(x[2])))[0]

        for idx, children, page in scored:
            action = "KEEP" if idx == best_idx else "REMOVE"
            report_rows.append({
                "record_type":   rec_type,
                "record_id":     rec_id,
                "context":       ctx,
                "source_ref":    sour_ref,
                "line_index":    idx,
                "child_lines":   children,
                "page_value":    page,
                "total_in_grp":  len(line_indices),
                "action":        action,
            })

    report_rows.sort(key=lambda r: (r["record_type"], r["record_id"],
                                    r["context"], r["source_ref"], r["line_index"]))
    return report_rows


def citations_write_report(report_rows, csv_path):
    fieldnames = [
        "record_type", "record_id", "context", "source_ref",
        "line_index", "child_lines", "page_value", "total_in_grp", "action",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    remove_count = sum(1 for r in report_rows if r["action"] == "REMOVE")
    keep_count   = sum(1 for r in report_rows if r["action"] == "KEEP")
    groups       = len(set((r["record_id"], r["context"], r["source_ref"])
                           for r in report_rows))
    _print(f"  Report written         : {csv_path}")
    _print(f"  Duplicate groups found : {groups}")
    _print(f"  Citations to KEEP      : {keep_count}")
    _print(f"  Citations to REMOVE    : {remove_count}")


def citations_print_summary(report_rows):
    groups = defaultdict(list)
    for row in report_rows:
        key = (row["record_type"], row["record_id"], row["context"], row["source_ref"])
        groups[key].append(row)

    _print(f"\n  {'RECORD':<14} {'CONTEXT':<14} {'SOURCE':<10} "
          f"{'OCCURRENCES':<12} {'KEEP LINE':<12} {'REMOVE LINES'}")
    _print("  " + "-" * 80)
    for (rec_type, rec_id, ctx, sour_ref), rows in sorted(groups.items()):
        keep_lines   = [r["line_index"] for r in rows if r["action"] == "KEEP"]
        remove_lines = [r["line_index"] for r in rows if r["action"] == "REMOVE"]
        label = f"{rec_type} {rec_id}"
        _print(f"  {label:<14} {ctx:<14} {sour_ref:<10} "
              f"{len(rows):<12} {str(keep_lines[0]) if keep_lines else '?':<12} "
              f"{remove_lines}")


def citations_apply(gedcom_lines, csv_path, output_path):
    remove_start_indices = set()
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            action = row.get("action", "REMOVE").strip().upper()
            if action != "REMOVE":
                continue
            try:
                idx = int(row["line_index"])
            except (KeyError, ValueError):
                print(f"  WARNING: Could not parse line_index in row: {row}", file=sys.stderr)
                continue
            remove_start_indices.add(idx)

    remove_lines = set()
    for start in remove_start_indices:
        block = get_block_lines(gedcom_lines, start)
        remove_lines.update(block)

    if not remove_lines:
        _print("  No lines flagged for removal. Output will be identical to input.")

    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            if idx in remove_lines:
                continue
            fh.write(raw + "\n")

    _print(f"  Removed {len(remove_start_indices)} citation block(s) ({len(remove_lines)} line(s) total).")
    _print(f"  Cleaned GEDCOM written: {output_path}")


def cmd_citations_detect(args):
    _print(f"\n=== CITATIONS: PHASE 1 — DETECT DUPLICATE SOURCE CITATIONS ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Scanning for duplicate citations...")
    rows = citations_detect(lines)
    if not rows:
        _print("  No duplicate source citations found.")
        return
    citations_write_report(rows, args.report)
    if not getattr(args, 'quiet', False):
        citations_print_summary(rows)
    _print(f"\n  Review the CSV. Change REMOVE to KEEP for any citation you wish to retain.")
    _print(f"  Then run:  python gedcom_cleanup.py citations apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_citations_apply(args):
    _print(f"\n=== CITATIONS: PHASE 2 — APPLY FIXES ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Applying fixes...")
    citations_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: PUBL
# Cleans corrupted PUBL (publisher) tags in GEDCOM files exported from Ancestry.
# ===========================================================================

def publ_collect_blocks(gedcom_lines):
    blocks = []
    n = len(gedcom_lines)
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if level == 1 and tag == "PUBL":
            block_idxs = [i]
            concat_val = rest
            j = i + 1
            while j < n:
                lv, tg, rt, _ = gedcom_lines[j]
                if lv is not None and lv > 1 and tg in ("CONC", "CONT"):
                    block_idxs.append(j)
                    concat_val += rt if tg == "CONC" else "\n" + rt
                    j += 1
                else:
                    break
            blocks.append({
                "start_idx":  i,
                "end_idx":    block_idxs[-1],
                "raw_value":  concat_val,
                "block_idxs": block_idxs,
            })
            i = j
        else:
            i += 1
    return blocks


def publ_clean_value(raw_value):
    val = raw_value
    # Fix 1: collapse repeated "Name: " prefixes
    val = re.sub(r'^(Name:\s*)+', '', val)
    # Fix 2: remove trailing semicolons and whitespace
    val = re.sub(r'[;\s]+$', '', val.strip())
    # Fix 3: fix spaces injected into 4-digit years after "Date: "
    def fix_year(m):
        digits = re.sub(r'\s+', '', m.group(1))
        return f"Date: {digits}"
    val = re.sub(r'Date:\s*([\d\s]{4,6}?)(?=[;,\s]|$)', fix_year, val)
    return val.strip()


def publ_detect(gedcom_lines, csv_path):
    blocks = publ_collect_blocks(gedcom_lines)
    rows = []
    changed = 0
    for b in blocks:
        raw   = b["raw_value"]
        clean = publ_clean_value(raw)
        needs_fix = (clean != raw.strip())
        if needs_fix:
            changed += 1
        rows.append({
            "start_line":  b["start_idx"] + 1,
            "end_line":    b["end_idx"] + 1,
            "raw_value":   raw,
            "clean_value": clean,
            "needs_fix":   "YES" if needs_fix else "NO",
            "action":      "FIX" if needs_fix else "SKIP",
        })

    fieldnames = ["start_line", "end_line", "raw_value", "clean_value", "needs_fix", "action"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    _print(f"  Report written  : {csv_path}")
    _print(f"  Total PUBL tags : {len(blocks)}")
    _print(f"  Need fixing     : {changed}")
    _print(f"  Already clean   : {len(blocks) - changed}")

    if changed:
        _print(f"\n  Sample fixes:")
        shown = 0
        for r in rows:
            if r["action"] == "FIX" and shown < 5:
                _print(f"    BEFORE: {repr(r['raw_value'][:80])}")
                _print(f"    AFTER : {repr(r['clean_value'][:80])}")
                _print()
                shown += 1
    return rows


def publ_apply(gedcom_lines, csv_path, output_path):
    fix_map = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            action = row.get("action", "SKIP").strip().upper()
            if action != "FIX":
                continue
            try:
                start = int(row["start_line"])
            except (KeyError, ValueError):
                print(f"  WARNING: Could not parse start_line in row: {row}", file=sys.stderr)
                continue
            fix_map[start] = row["clean_value"]

    if not fix_map:
        _print("  No FIX actions found. Output will be identical to input.")

    blocks = publ_collect_blocks(gedcom_lines)
    skip_indices    = set()
    replacement_map = {}
    fixed_count = 0

    for b in blocks:
        start_1based = b["start_idx"] + 1
        if start_1based not in fix_map:
            continue
        replacement_map[b["start_idx"]] = f"1 PUBL {fix_map[start_1based]}"
        for idx in b["block_idxs"][1:]:
            skip_indices.add(idx)
        fixed_count += 1

    lines_replaced = 0
    lines_removed  = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            if idx in skip_indices:
                lines_removed += 1
                continue
            if idx in replacement_map:
                fh.write(replacement_map[idx] + "\n")
                lines_replaced += 1
            else:
                fh.write(raw + "\n")

    _print(f"  PUBL blocks fixed       : {fixed_count}")
    _print(f"  PUBL lines replaced     : {lines_replaced}")
    _print(f"  CONC/CONT lines removed : {lines_removed}")
    _print(f"  Cleaned GEDCOM written  : {output_path}")


def cmd_publ_detect(args):
    _print(f"\n=== PUBL: PHASE 1 — DETECT PUBL CORRUPTION ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Scanning PUBL tags...")
    publ_detect(lines, args.report)
    _print(f"\n  Review the CSV. Change FIX to SKIP for any entries to leave unchanged.")
    _print(f"  Then run:  python gedcom_cleanup.py publ apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_publ_apply(args):
    _print(f"\n=== PUBL: PHASE 2 — APPLY PUBL FIXES ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Applying fixes...")
    publ_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: RESI_DATES
# Infers missing DATE values for RESI events from source citations.
# ===========================================================================

RESI_INFERENCE_RULES = [
    (r'\b1790\b.*[Cc]ensus',   "1790", "EXACT"),
    (r'\b1800\b.*[Cc]ensus',   "1800", "EXACT"),
    (r'\b1810\b.*[Cc]ensus',   "1810", "EXACT"),
    (r'\b1820\b.*[Cc]ensus',   "1820", "EXACT"),
    (r'\b1830\b.*[Cc]ensus',   "1830", "EXACT"),
    (r'\b1840\b.*[Cc]ensus',   "1840", "EXACT"),
    (r'\b1850\b.*[Cc]ensus',   "1850", "EXACT"),
    (r'\b1860\b.*[Cc]ensus',   "1860", "EXACT"),
    (r'\b1870\b.*[Cc]ensus',   "1870", "EXACT"),
    (r'\b1880\b.*[Cc]ensus',   "1880", "EXACT"),
    (r'\b1890\b.*[Cc]ensus',   "1890", "EXACT"),
    (r'\b1900\b.*[Cc]ensus',   "1900", "EXACT"),
    (r'\b1910\b.*[Cc]ensus',   "1910", "EXACT"),
    (r'\b1920\b.*[Cc]ensus',   "1920", "EXACT"),
    (r'\b1930\b.*[Cc]ensus',   "1930", "EXACT"),
    (r'\b1940\b.*[Cc]ensus',   "1940", "EXACT"),
    (r'\b1950\b.*[Cc]ensus',   "1950", "EXACT"),
    (r'\b1960\b.*[Cc]ensus',   "1960", "EXACT"),
    (r'World War II Draft.*194[0-7]',         "ABT 1942", "APPROX"),
    (r'WWII Draft',                           "ABT 1942", "APPROX"),
    (r'World War II Army Enlistment',         "ABT 1942", "APPROX"),
    (r'World War II.*Draft Cards Young Men',  "ABT 1942", "APPROX"),
    (r'1917-1918',                            "ABT 1917", "APPROX"),
    (r'World War I',                          "ABT 1917", "APPROX"),
    (r'Maryland Military Men.*1917',          "ABT 1917", "APPROX"),
    (r'World War I Draft',                    "ABT 1918", "APPROX"),
]


def resi_infer_date_from_title(titl):
    for pattern, date_val, conf in RESI_INFERENCE_RULES:
        if re.search(pattern, titl):
            return date_val, conf
    return None, None


def resi_build_source_date_map(gedcom_lines):
    n = len(gedcom_lines)
    source_map = {}
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if level == 0 and tag and is_pointer(tag) and rest == "SOUR":
            sour_id = tag
            titl = ""
            j = i + 1
            while j < n:
                lv, tg, rt, _ = gedcom_lines[j]
                if lv == 0:
                    break
                if lv == 1 and tg == "TITL":
                    titl = rt
                    k = j + 1
                    while k < n:
                        lv2, tg2, rt2, _ = gedcom_lines[k]
                        if lv2 is not None and lv2 > 1 and tg2 in ("CONC", "CONT"):
                            titl += rt2
                            k += 1
                        else:
                            break
                    break
                j += 1
            date_val, confidence = resi_infer_date_from_title(titl)
            if date_val:
                source_map[sour_id] = {"date": date_val, "confidence": confidence, "title": titl}
            i = j
        else:
            i += 1
    return source_map


def resi_detect(gedcom_lines, csv_path):
    source_map = resi_build_source_date_map(gedcom_lines)
    n = len(gedcom_lines)
    current_rec_type = None
    current_rec_id   = None
    rows = []
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if level == 0:
            current_rec_type = None
            current_rec_id   = None
            if tag and is_pointer(tag) and rest in ("INDI", "FAM"):
                current_rec_type = rest
                current_rec_id   = tag
            i += 1
            continue
        if current_rec_type is None:
            i += 1
            continue
        if level == 1 and tag == "RESI":
            resi_start = i
            has_date   = False
            place      = ""
            sour_refs  = []
            page_vals  = {}
            j = i + 1
            while j < n:
                lv, tg, rt, _ = gedcom_lines[j]
                if lv is None or lv <= 1:
                    break
                if lv == 2:
                    if tg == "DATE":
                        has_date = True
                    elif tg == "PLAC":
                        place = rt.strip()
                    elif tg == "SOUR" and is_pointer(rt):
                        sref = rt.strip()
                        sour_refs.append(sref)
                        k = j + 1
                        while k < n:
                            lv2, tg2, rt2, _ = gedcom_lines[k]
                            if lv2 is None or lv2 <= 2:
                                break
                            if lv2 == 3 and tg2 == "PAGE":
                                page_vals[sref] = rt2.strip()
                            k += 1
                j += 1
            if not has_date and sour_refs:
                best = None
                for sref in sour_refs:
                    if sref in source_map:
                        entry = source_map[sref]
                        if best is None or entry["confidence"] == "EXACT":
                            best = (sref, entry)
                        if best[1]["confidence"] == "EXACT":
                            break
                if best:
                    sref, entry = best
                    rows.append({
                        "record_type":   current_rec_type,
                        "record_id":     current_rec_id,
                        "resi_line":     resi_start + 1,
                        "place":         place,
                        "inferred_date": entry["date"],
                        "confidence":    entry["confidence"],
                        "from_source":   sref,
                        "source_title":  entry["title"][:80],
                        "page_value":    page_vals.get(sref, ""),
                        "action":        "ADD",
                    })
        i += 1

    fieldnames = [
        "record_type", "record_id", "resi_line", "place",
        "inferred_date", "confidence", "from_source", "source_title",
        "page_value", "action",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    exact  = sum(1 for r in rows if r["confidence"] == "EXACT")
    approx = sum(1 for r in rows if r["confidence"] == "APPROX")
    _print(f"  Report written              : {csv_path}")
    _print(f"  Inferrable RESI dates found : {len(rows)}")
    _print(f"    EXACT  (plain year)       : {exact}")
    _print(f"    APPROX (ABT qualifier)    : {approx}")

    if rows:
        _print(f"\n  Preview:")
        _print(f"  {'RECORD':<14} {'RESI LINE':<11} {'DATE':<14} {'CONF':<8} {'PLACE'}")
        _print("  " + "-" * 72)
        for r in rows:
            _print(f"  {r['record_type']+' '+r['record_id']:<14} "
                  f"line {r['resi_line']:<6} "
                  f"{r['inferred_date']:<14} "
                  f"{r['confidence']:<8} "
                  f"{r['place'][:35]}")
    return rows


def resi_apply(gedcom_lines, csv_path, output_path):
    insertions = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            action = row.get("action", "ADD").strip().upper()
            if action != "ADD":
                continue
            try:
                line_no = int(row["resi_line"])
            except (KeyError, ValueError):
                print(f"  WARNING: Could not parse resi_line: {row}", file=sys.stderr)
                continue
            insertions[line_no] = row["inferred_date"].strip()

    if not insertions:
        _print("  No ADD actions found. Output will be identical to input.")

    added = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            fh.write(raw + "\n")
            line_no = idx + 1
            if line_no in insertions:
                fh.write(f"2 DATE {insertions[line_no]}\n")
                added += 1

    _print(f"  DATE lines inserted   : {added}")
    _print(f"  Cleaned GEDCOM written: {output_path}")


def cmd_resi_dates_detect(args):
    _print(f"\n=== RESI_DATES: PHASE 1 — DETECT INFERRABLE RESI DATES ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Scanning RESI events...")
    resi_detect(lines, args.report)
    _print(f"\n  Review the CSV. Change ADD to SKIP for any dates you do not wish to add.")
    _print(f"  Then run:  python gedcom_cleanup.py resi_dates apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_resi_dates_apply(args):
    _print(f"\n=== RESI_DATES: PHASE 2 — APPLY RESI DATE INSERTIONS ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Applying insertions...")
    resi_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: MEDIA
# Detects and removes duplicate OBJE (media) attachments.
# ===========================================================================

def media_detect(gedcom_lines):
    occurrences = defaultdict(list)
    i = 0
    n = len(gedcom_lines)
    current_record_type = None
    current_record_id   = None
    current_event_tag   = None
    current_event_level = None
    current_sour_ref    = None
    current_sour_level  = None

    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if level is None:
            i += 1
            continue

        if level == 0:
            current_record_type = None
            current_record_id   = None
            current_event_tag   = None
            current_event_level = None
            current_sour_ref    = None
            current_sour_level  = None
            if is_pointer(tag) and rest in TOP_RECORDS:
                current_record_type = rest
                current_record_id   = tag
            i += 1
            continue

        if current_record_type is None:
            i += 1
            continue

        if level == 1 and tag in EVENT_TAGS:
            current_event_tag   = tag
            current_event_level = level
            current_sour_ref    = None
            current_sour_level  = None
        elif current_event_level is not None and level <= current_event_level:
            current_event_tag   = None
            current_event_level = None

        if tag == "SOUR" and is_pointer(rest):
            current_sour_ref   = rest
            current_sour_level = level
        elif current_sour_level is not None and level <= current_sour_level and not (tag == "SOUR" and is_pointer(rest)):
            current_sour_ref   = None
            current_sour_level = None

        if tag == "OBJE":
            obje_ref = None
            dup_type = None
            if is_pointer(rest):
                obje_ref = rest
                dup_type = "POINTER"
            else:
                j = i + 1
                while j < n and gedcom_lines[j][0] is not None and gedcom_lines[j][0] > level:
                    if gedcom_lines[j][1] == "FILE":
                        obje_ref = gedcom_lines[j][2].strip()
                        dup_type = "FILEPATH"
                        break
                    j += 1
            if obje_ref:
                if current_sour_ref:
                    ctx_detail = f"SOUR {current_sour_ref}"
                elif current_event_tag:
                    ctx_detail = current_event_tag
                else:
                    ctx_detail = "(top-level)"
                key = (current_record_type, current_record_id, ctx_detail, obje_ref, dup_type)
                occurrences[key].append(i)

        i += 1

    report_rows = []
    for key, line_indices in occurrences.items():
        if len(line_indices) < 2:
            continue
        ctx_type, ctx_id, ctx_detail, obje_ref, dup_type = key
        count = len(line_indices)
        for idx in line_indices[1:]:
            report_rows.append({
                "context_type":    ctx_type,
                "context_id":      ctx_id,
                "context_detail":  ctx_detail,
                "obje_ref":        obje_ref,
                "duplicate_count": count,
                "duplicate_type":  dup_type,
                "line_index":      idx,
                "action":          "REMOVE",
            })
    return report_rows


def media_write_report(report_rows, csv_path):
    fieldnames = [
        "context_type", "context_id", "context_detail",
        "obje_ref", "duplicate_count", "duplicate_type",
        "line_index", "action",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)
    _print(f"  Report written: {csv_path}  ({len(report_rows)} duplicate attachment(s) found)")


def media_apply(gedcom_lines, csv_path, output_path):
    """
    Apply media duplicate removals.

    The CSV records which (context, obje_ref) pairs should be removed, but
    the line_index values in the CSV were generated against the original
    GEDCOM and are stale by the time this tool runs (earlier tools have
    already removed lines, shifting all indices).

    Fix: re-run media_detect on the current gedcom_lines to get fresh
    indices, then cross-reference against the CSV to honour any KEEP
    overrides the user may have set.
    """
    # Load the user-reviewed CSV to find which (context_id, obje_ref) to keep
    keep_keys = set()   # (context_id, context_detail, obje_ref) -> keep
    remove_keys = set()
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            action = row.get("action", "REMOVE").strip().upper()
            key = (
                row.get("context_id", "").strip(),
                row.get("context_detail", "").strip(),
                row.get("obje_ref", "").strip(),
            )
            if action == "REMOVE":
                remove_keys.add(key)
            else:
                keep_keys.add(key)

    # Re-detect on the current (shifted) file to get fresh line indices
    fresh_rows = media_detect(gedcom_lines)

    remove_indices = set()
    for row in fresh_rows:
        key = (
            row["context_id"].strip(),
            row["context_detail"].strip(),
            row["obje_ref"].strip(),
        )
        # Only remove if the CSV flagged this key for removal and user hasn't
        # overridden it to KEEP
        if key in remove_keys and key not in keep_keys:
            block = get_block_lines(gedcom_lines, row["line_index"])
            remove_indices.update(block)

    if not remove_indices:
        _print("  No lines to remove. Output file will be identical to input.")

    removed = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            if idx in remove_indices:
                removed += 1
                continue
            fh.write(raw + "\n")

    _print(f"  Removed {removed} line(s) across duplicate OBJE block(s).")
    _print(f"  Cleaned GEDCOM written: {output_path}")


def cmd_media_detect(args):
    _print(f"\n=== MEDIA: PHASE 1 — DETECT DUPLICATE MEDIA ATTACHMENTS ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Scanning for duplicate OBJE attachments...")
    rows = media_detect(lines)
    if not rows:
        _print("  No duplicate media attachments found.")
    else:
        media_write_report(rows, args.report)
        _print(f"\n  Review the CSV. Change REMOVE to KEEP for any duplicates to retain.")
        _print(f"  Then run:  python gedcom_cleanup.py media apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_media_apply(args):
    _print(f"\n=== MEDIA: PHASE 2 — APPLY MEDIA FIXES ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    _print("  Parsing GEDCOM...")
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.")
    _print("  Applying fixes...")
    media_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: NOPHOTO
# Lists all INDI records that have no _PHOTO tag anywhere in their block.
# Detect-only — no apply phase.
# ===========================================================================

def nophoto_detect(gedcom_lines):
    """Walk GEDCOM; return list of INDI records with no _PHOTO tag."""
    n = len(gedcom_lines)
    results = []
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if level == 0 and tag and is_pointer(tag) and rest and rest.strip() == "INDI":
            indi_id   = tag
            indi_line = i + 1
            name      = ""
            has_photo = False
            j = i + 1
            while j < n:
                lv, tg, rs, _ = gedcom_lines[j]
                if lv is None:
                    j += 1
                    continue
                if lv == 0:
                    break
                if tg == "NAME" and not name:
                    name = rs.strip() if rs else ""
                if tg == "_PHOTO":
                    has_photo = True
                j += 1
            if not has_photo:
                results.append({"indi_id": indi_id, "name": name, "line_number": indi_line})
            i = j
            continue
        i += 1
    return results


def nophoto_write_report(report_rows, csv_path):
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["indi_id", "name", "line_number"])
        writer.writeheader()
        writer.writerows(report_rows)
    _print(f"  Report written: {csv_path}  ({len(report_rows)} individual(s) without _PHOTO)")


def cmd_nophoto_detect(args):
    _print(f"\n=== NOPHOTO: DETECT INDIVIDUALS WITHOUT _PHOTO TAG ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Scanning...")
    rows = nophoto_detect(lines)
    if not rows:
        _print("  All individuals have a _PHOTO tag — nothing to report.")
    else:
        nophoto_write_report(rows, args.report)


# ===========================================================================
# TOOL: MISSING_MEDIA
# Scans GEDCOM FILE tags and checks whether each referenced file exists on
# disk in the media folder.  Detect only — no apply phase.
# ===========================================================================

def missing_media_detect(gedcom_lines, media_dir):
    """
    Scan all FILE tags in the GEDCOM and check whether the referenced file
    exists in media_dir.  Returns a list of dicts for missing files.
    """
    import os
    media_dir = os.path.abspath(media_dir)
    missing = []
    seen = {}  # filename -> first line number seen

    for i, (level, tag, rest, raw) in enumerate(gedcom_lines):
        if level is None or tag != 'FILE':
            continue
        filepath = rest.strip()
        if not filepath:
            continue

        # Normalize: strip any leading path components, keep just the filename
        filename = os.path.basename(filepath.replace('\\', '/'))
        full_path = os.path.join(media_dir, filename)

        if filename in seen:
            continue  # already reported this filename
        seen[filename] = i

        if not os.path.exists(full_path):
            missing.append({
                'line_number': i + 1,
                'gedcom_file_tag': filepath,
                'filename': filename,
                'expected_path': full_path,
            })

    return missing


def missing_media_write_report(rows, csv_path):
    import csv
    fieldnames = ['line_number', 'gedcom_file_tag', 'filename', 'expected_path']
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    _print(f"  Report written: {csv_path}  ({len(rows)} missing files)")


def cmd_missing_media_detect(args):
    import os
    _print(f"\n=== MISSING_MEDIA: DETECT MEDIA FILES NOT FOUND ON DISK ===")
    _print(f"  Input     : {args.input}")
    _print(f"  Media dir : {args.media_dir}")
    _print(f"  Report    : {args.report}")
    require_files(args.input)
    if not os.path.isdir(args.media_dir):
        print(f"  ERROR: Media directory not found: {args.media_dir}", file=sys.stderr)
        return
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Scanning FILE tags...")
    rows = missing_media_detect(lines, args.media_dir)
    if not rows:
        _print("  All FILE references found on disk — nothing missing.")
    else:
        _print(f"  {len(rows)} missing file(s) detected.")
        missing_media_write_report(rows, args.report)


# ===========================================================================
# TOOL: MOJIBAKE
# Fixes common UTF-8 mojibake artifacts (e.g. â€™ → ') caused by double-
# encoding during Ancestry/FTM exports.  No detect phase — always applied
# as a passive pass during any apply run.  A detect phase is provided so
# users can review what would be changed before committing.
# ===========================================================================

_MOJIBAKE_MAP = {
    "\xe2\x80\x99": "\u2019",   # â€™ → right single quote
    "\xe2\x80\x9c": "\u201c",   # â€œ → left double quote
    "\xe2\x80\x9d": "\u201d",   # â€  → right double quote
    "\xe2\x80\x93": "\u2013",   # â€" → en dash
    "\xe2\x80\x94": "\u2014",   # â€" → em dash
    "\xe2\x80\xa6": "\u2026",   # â€¦ → ellipsis
    "\xc3\xa9": "\xe9",         # Ã© → é
    "\xc3\xa8": "\xe8",         # Ã¨ → è
    "\xc3\xa2": "\xe2",         # Ã¢ → â
    "\xc3\xb4": "\xf4",         # Ã´ → ô
    "\xc3\xb6": "\xf6",         # Ã¶ → ö
    "\xc3\xbc": "\xfc",         # Ã¼ → ü
    "\xc3\xb1": "\xf1",         # Ã± → ñ
}


def mojibake_fix_line(raw):
    """Return (fixed_line, changed) after applying all mojibake substitutions."""
    result = raw
    for bad, good in _MOJIBAKE_MAP.items():
        if bad in result:
            result = result.replace(bad, good)
    return result, result != raw


def mojibake_detect(gedcom_lines, csv_path):
    """Scan for mojibake and write a report CSV."""
    rows = []
    for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
        fixed, changed = mojibake_fix_line(raw)
        if changed:
            rows.append({
                "line_number": idx + 1,
                "original":    raw,
                "fixed":       fixed,
                "action":      "FIX",
            })

    fieldnames = ["line_number", "original", "fixed", "action"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    _print(f"  Report written         : {csv_path}")
    _print(f"  Lines with mojibake    : {len(rows)}")
    return rows


def mojibake_apply(gedcom_lines, csv_path, output_path):
    """Apply mojibake fixes from the report CSV."""
    fix_lines = set()
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("action", "FIX").strip().upper() == "FIX":
                try:
                    fix_lines.add(int(row["line_number"]))
                except (KeyError, ValueError):
                    pass

    fixed = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            line_no = idx + 1
            if line_no in fix_lines:
                result, _ = mojibake_fix_line(raw)
                fh.write(result + "\n")
                fixed += 1
            else:
                fh.write(raw + "\n")

    _print(f"  Lines fixed            : {fixed}")
    _print(f"  Cleaned GEDCOM written : {output_path}")


def cmd_mojibake_detect(args):
    _print(f"\n=== MOJIBAKE: PHASE 1 — DETECT ENCODING ARTIFACTS ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Scanning...")
    rows = mojibake_detect(lines, args.report)
    if not rows:
        _print("  No mojibake artifacts found.")
    else:
        _print(f"\n  Review the CSV. Change FIX to SKIP for any lines to leave unchanged.")
        _print(f"  Then run:  python gedcom_cleanup.py mojibake apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_mojibake_apply(args):
    _print(f"\n=== MOJIBAKE: PHASE 2 — APPLY ENCODING FIXES ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Applying fixes...")
    mojibake_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: DATES
# Detects and normalizes numeric dates (MM/DD/YYYY or MM-DD-YYYY) to the
# GEDCOM standard format (DD MON YYYY), e.g. 03/15/1942 → 15 MAR 1942.
# ===========================================================================

_MONTHS = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}

_DATE_NUMERIC_RE = re.compile(r'^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s*$')


def _normalize_numeric_date(value, date_style="us"):
    """Convert MM/DD/YYYY or DD/MM/YYYY to DD MON YYYY.  Returns None if no match."""
    m = _DATE_NUMERIC_RE.match(value)
    if not m:
        return None
    a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    month, day = (a, b) if date_style == "us" else (b, a)
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    mon = _MONTHS.get(month)
    if not mon:
        return None
    return f"{day:02d} {mon} {y}"


def dates_detect(gedcom_lines, csv_path, date_style="us"):
    """Scan DATE tags for numeric format and write a report CSV."""
    rows = []
    for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
        if tag and tag.upper() == "DATE" and rest:
            norm = _normalize_numeric_date(rest, date_style)
            if norm:
                rows.append({
                    "line_number":  idx + 1,
                    "original":     rest,
                    "normalized":   norm,
                    "action":       "FIX",
                })

    fieldnames = ["line_number", "original", "normalized", "action"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    _print(f"  Report written         : {csv_path}")
    _print(f"  Numeric dates found    : {len(rows)}")
    return rows


def dates_apply(gedcom_lines, csv_path, output_path):
    """Apply date normalizations from the report CSV."""
    fix_map = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("action", "FIX").strip().upper() == "FIX":
                try:
                    fix_map[int(row["line_number"])] = row["normalized"].strip()
                except (KeyError, ValueError):
                    pass

    fixed = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            line_no = idx + 1
            if line_no in fix_map and tag and tag.upper() == "DATE":
                fh.write(f"{level} DATE {fix_map[line_no]}\n")
                fixed += 1
            else:
                fh.write(raw + "\n")

    _print(f"  DATE lines normalized  : {fixed}")
    _print(f"  Cleaned GEDCOM written : {output_path}")


def cmd_dates_detect(args):
    _print(f"\n=== DATES: PHASE 1 — DETECT NUMERIC DATE FORMAT ===")
    _print(f"  Input      : {args.input}")
    _print(f"  Report     : {args.report}")
    _print(f"  Date style : {getattr(args, 'date_style', 'us')}")
    require_files(args.input)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Scanning...")
    rows = dates_detect(lines, args.report, getattr(args, 'date_style', 'us'))
    if not rows:
        _print("  No numeric dates found.")
    else:
        _print(f"\n  Review the CSV. Change FIX to SKIP for any dates to leave unchanged.")
        _print(f"  Then run:  python gedcom_cleanup.py dates apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_dates_apply(args):
    _print(f"\n=== DATES: PHASE 2 — APPLY DATE NORMALIZATION ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Applying fixes...")
    dates_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: SEX
# Detects INDI records with no SEX tag and inserts '1 SEX U'.
# RootsMagic drops SEX U on export; this restores it so every individual
# has an explicit sex value.
# ===========================================================================

def sex_detect(gedcom_lines, csv_path):
    """Find INDI records missing a SEX tag and write a report CSV."""
    rows = []
    n = len(gedcom_lines)
    i = 0
    while i < n:
        level, tag, rest, raw = gedcom_lines[i]
        if level == 0 and tag and is_pointer(tag) and rest and rest.strip() == "INDI":
            indi_id   = tag
            indi_line = i + 1
            name      = ""
            has_sex   = False
            j = i + 1
            while j < n:
                lv, tg, rs, _ = gedcom_lines[j]
                if lv is None:
                    j += 1
                    continue
                if lv == 0:
                    break
                if lv == 1 and tg == "SEX":
                    has_sex = True
                if tg == "NAME" and not name:
                    name = rs.strip() if rs else ""
                j += 1
            if not has_sex:
                rows.append({
                    "indi_id":     indi_id,
                    "name":        name,
                    "line_number": indi_line,
                    "action":      "ADD",
                })
            i = j
            continue
        i += 1

    fieldnames = ["indi_id", "name", "line_number", "action"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    _print(f"  Report written         : {csv_path}")
    _print(f"  Individuals missing SEX: {len(rows)}")
    return rows


def sex_apply(gedcom_lines, csv_path, output_path):
    """Insert '1 SEX U' after the first NAME line for individuals missing SEX."""
    add_sex = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("action", "ADD").strip().upper() == "ADD":
                try:
                    add_sex[row["indi_id"].strip()] = int(row["line_number"])
                except (KeyError, ValueError):
                    pass

    if not add_sex:
        _print("  No ADD actions found. Output will be identical to input.")

    added = 0
    n = len(gedcom_lines)
    i = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        while i < n:
            level, tag, rest, raw = gedcom_lines[i]
            fh.write(raw + "\n")

            # If this is the start of an INDI record that needs SEX added,
            # find the right insertion point (after the first NAME block)
            if (level == 0 and tag and is_pointer(tag)
                    and rest and rest.strip() == "INDI"
                    and tag.strip() in add_sex):

                # Scan ahead for NAME block end
                insert_after = i  # fallback: insert right after 0-line
                j = i + 1
                in_name = False
                while j < n:
                    lv, tg, rs, _ = gedcom_lines[j]
                    if lv is None:
                        j += 1
                        continue
                    if lv == 0:
                        break
                    if lv == 1 and tg == "NAME":
                        in_name = True
                        insert_after = j
                        j += 1
                        continue
                    if in_name:
                        if lv >= 2:
                            insert_after = j
                            j += 1
                            continue
                        else:
                            # Back to level 1 — end of NAME block
                            break
                    j += 1

                # Write everything up to and including insert_after
                for k in range(i + 1, insert_after + 1):
                    fh.write(gedcom_lines[k][3] + "\n")
                fh.write("1 SEX U\n")
                added += 1
                i = insert_after + 1
                continue

            i += 1

    _print(f"  SEX U lines inserted   : {added}")
    _print(f"  Cleaned GEDCOM written : {output_path}")


def cmd_sex_detect(args):
    _print(f"\n=== SEX: PHASE 1 — DETECT INDIVIDUALS MISSING SEX TAG ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    require_files(args.input)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Scanning...")
    rows = sex_detect(lines, args.report)
    if not rows:
        _print("  All individuals have a SEX tag.")
    else:
        _print(f"\n  Review the CSV. Change ADD to SKIP for any individuals to leave unchanged.")
        _print(f"  Then run:  python gedcom_cleanup.py sex apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_sex_apply(args):
    _print(f"\n=== SEX: PHASE 2 — APPLY SEX TAG INSERTIONS ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Applying insertions...")
    sex_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# TOOL: FILE_PATHS
# Detects FILE tags whose paths don't use the canonical 'media/' subdir
# prefix and rewrites them.  Handles backslashes, absolute paths, and
# bare filenames (no directory prefix).
# ===========================================================================

_FILE_TAG_RE = re.compile(r'^(?P<level>\d+)\s+FILE\s+(?P<path>.+)$')


def _normalize_file_path(path, media_subdir="media"):
    """Return (new_path, changed).  Rewrites path to media_subdir/basename."""
    path = path.replace("\\", "/").strip()
    base = os.path.basename(path)
    if not base:
        return path, False
    new_path = f"{media_subdir.rstrip('/')}/{base}"
    return new_path, new_path != path


def file_paths_detect(gedcom_lines, csv_path, media_subdir="media"):
    """Scan FILE tags and write a report of paths that need rewriting."""
    rows = []
    for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
        if tag and tag.upper() == "FILE" and rest:
            new_path, changed = _normalize_file_path(rest, media_subdir)
            if changed:
                rows.append({
                    "line_number":  idx + 1,
                    "original":     rest,
                    "normalized":   new_path,
                    "action":       "FIX",
                })

    fieldnames = ["line_number", "original", "normalized", "action"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    _print(f"  Report written         : {csv_path}")
    _print(f"  FILE paths to rewrite  : {len(rows)}")
    return rows


def file_paths_apply(gedcom_lines, csv_path, output_path):
    """Rewrite FILE paths from the report CSV."""
    fix_map = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("action", "FIX").strip().upper() == "FIX":
                try:
                    fix_map[int(row["line_number"])] = row["normalized"].strip()
                except (KeyError, ValueError):
                    pass

    fixed = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(gedcom_lines):
            line_no = idx + 1
            if line_no in fix_map and tag and tag.upper() == "FILE":
                fh.write(f"{level} FILE {fix_map[line_no]}\n")
                fixed += 1
            else:
                fh.write(raw + "\n")

    _print(f"  FILE paths rewritten   : {fixed}")
    _print(f"  Cleaned GEDCOM written : {output_path}")


def cmd_file_paths_detect(args):
    _print(f"\n=== FILE_PATHS: PHASE 1 — DETECT NON-STANDARD MEDIA FILE PATHS ===")
    _print(f"  Input        : {args.input}")
    _print(f"  Report       : {args.report}")
    _print(f"  Media subdir : {getattr(args, 'media_subdir', 'media')}")
    require_files(args.input)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Scanning...")
    rows = file_paths_detect(lines, args.report, getattr(args, 'media_subdir', 'media'))
    if not rows:
        _print("  All FILE paths already use the correct media subdir.")
    else:
        _print(f"\n  Review the CSV. Change FIX to SKIP for any paths to leave unchanged.")
        _print(f"  Then run:  python gedcom_cleanup.py file_paths apply --input {args.input} --report {args.report} --output <out.ged>")


def cmd_file_paths_apply(args):
    _print(f"\n=== FILE_PATHS: PHASE 2 — APPLY FILE PATH REWRITES ===")
    _print(f"  Input : {args.input}")
    _print(f"  Report: {args.report}")
    _print(f"  Output: {args.output}")
    require_files(args.input, args.report)
    require_distinct(args.input, args.output)
    lines = read_gedcom_lines(args.input)
    _print(f"  Read {len(lines):,} lines.  Applying rewrites...")
    file_paths_apply(lines, args.report, args.output)
    _print("\n  Done.")


# ===========================================================================
# PROJECT PATHS  (read from config/website_config.ini)
# ===========================================================================

_SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
_CONFIG_FILE_DEFAULT = os.path.normpath(
    os.path.join(_SCRIPT_DIR, '..', 'config', 'website_config.ini')
)


def load_project_paths(config_path=None):
    """Read [Paths] from website_config.ini. Returns {} with warning if missing.
    If config_path is supplied it overrides the default location."""
    cfg_file = config_path if config_path else _CONFIG_FILE_DEFAULT
    if not os.path.isfile(cfg_file):
        print(f"  WARNING: Config file not found: {cfg_file}", file=sys.stderr)
        _print("  Project paths will not be available.")
        return {}
    cfg = configparser.ConfigParser()
    cfg.read(cfg_file)
    if not cfg.has_section('Paths'):
        print(f"  WARNING: No [Paths] section in {cfg_file}", file=sys.stderr)
        return {}
    return {
        # Media
        'media_source':          cfg.get('Paths', 'Media_Source',          fallback=''),
        'media':                 cfg.get('Paths', 'Media',                 fallback=''),
        # Special images
        'special_images_source': cfg.get('Paths', 'Special_Images_Source', fallback=''),
        'special_images':        cfg.get('Paths', 'Special_Images',        fallback=''),
        # ResultsList.plist
        'results_list_source':   cfg.get('Paths', 'ResultsList_Source',    fallback=''),
        'results_list':          cfg.get('Paths', 'ResultsList',           fallback=''),
        # GEDCOM folder
        'gedcom_source':         cfg.get('Paths', 'Gedcom_Source',         fallback=''),
        'gedcom':                cfg.get('Paths', 'Gedcom',                fallback=''),
        # Styles folder
        'styles_source':         cfg.get('Paths', 'Styles_Source',         fallback=''),
        'styles':                cfg.get('Paths', 'Styles',                fallback=''),
        # Assets (logo.gif, Template.jpg)
        'assets_source':         cfg.get('Paths', 'Assets_Source',         fallback=''),
        'assets':                cfg.get('Paths', 'Assets',                fallback=''),
        # Config file
        'config_source':         cfg.get('Paths', 'Config_Source',         fallback=''),
        'config':                cfg.get('Paths', 'Config',                fallback=''),
    }


def print_project_paths(paths):
    if not paths:
        return
    _print("\nProject paths (from website_config.ini):")
    _print(f"  Media source       : {paths.get('media_source')          or '(not set)'}")
    _print(f"  Media dest         : {paths.get('media')                 or '(not set)'}")
    _print(f"  Special images src : {paths.get('special_images_source') or '(not set)'}")
    _print(f"  Special images dest: {paths.get('special_images')        or '(not set)'}")
    _print(f"  ResultsList source : {paths.get('results_list_source')   or '(not set)'}")
    _print(f"  ResultsList dest   : {paths.get('results_list')          or '(not set)'}")
    _print(f"  GEDCOM source      : {paths.get('gedcom_source')         or '(not set)'}")
    _print(f"  GEDCOM dest folder : {paths.get('gedcom')                or '(not set)'}")
    _print(f"  Styles source      : {paths.get('styles_source')         or '(not set)'}")
    _print(f"  Styles dest        : {paths.get('styles')                or '(not set)'}")
    _print(f"  Assets source      : {paths.get('assets_source')         or '(not set)'}")
    _print(f"  Assets dest        : {paths.get('assets')                or '(not set)'}")
    _print(f"  Config source      : {paths.get('config_source')         or '(not set)'}")
    _print(f"  Config dest        : {paths.get('config')                or '(not set)'}")


def _copy_folder_contents(src_dir, dest_dir, label):
    """Copy all files from src_dir into dest_dir. Source and dest must differ."""
    if not src_dir:
        _print(f"  {label}: source path not configured, skipping.")
        return 0
    if not os.path.isdir(src_dir):
        print(f"  WARNING: {label} source folder not found: {src_dir}", file=sys.stderr)
        return 0
    if not dest_dir:
        _print(f"  {label}: destination path not configured, skipping.")
        return 0
    if not os.path.isdir(dest_dir):
        print(f"  WARNING: {label} destination folder not found: {dest_dir}", file=sys.stderr)
        return 0
    if os.path.abspath(src_dir) == os.path.abspath(dest_dir):
        _print(f"  {label}: source and destination are the same folder, skipping.")
        return 0
    copied = overwritten = 0
    for fname in os.listdir(src_dir):
        src_file  = os.path.join(src_dir,  fname)
        dest_file = os.path.join(dest_dir, fname)
        if not os.path.isfile(src_file):
            continue
        if os.path.exists(dest_file):
            overwritten += 1
        shutil.copy2(src_file, dest_file)
        copied += 1
    _print(f"  {label}: {copied} file(s) copied ({overwritten} overwritten)  →  {dest_dir}")
    return copied


def _copy_single_file(src_file, dest_path, label):
    """Copy a single file to dest_path (file path or folder). Skips if same file."""
    if not src_file:
        _print(f"  {label}: source path not configured, skipping.")
        return
    if not os.path.isfile(src_file):
        print(f"  WARNING: {label} source file not found: {src_file}", file=sys.stderr)
        return
    dest_file = (os.path.join(dest_path, os.path.basename(src_file))
                 if os.path.isdir(dest_path) else dest_path)
    if os.path.abspath(src_file) == os.path.abspath(dest_file):
        _print(f"  {label}: source and destination are the same file, skipping.")
        return
    dest_dir = os.path.dirname(dest_file)
    if dest_dir and not os.path.isdir(dest_dir):
        print(f"  WARNING: {label} destination folder not found: {dest_dir}", file=sys.stderr)
        return
    existed = os.path.exists(dest_file)
    shutil.copy2(src_file, dest_file)
    _print(f"  {label}: copied{' (overwritten)' if existed else ''}  →  {dest_file}")


def sync_project_files(paths):
    """
    Copy all project source files to their configured destinations.
    Runs at startup so cleanup tools always work with the latest files.

    Copies:
      Test_Data/Test_Media/        →  grasp_final/media/
      Test_Data/special_images/    →  grasp_final/special_images/
      Test_Data/styles/            →  grasp_final/styles/
      Test_Data/ResultsList.plist  →  grasp_final/ResultsList.plist
      Test_Data/logo.gif           →  grasp_final/assets/
      Test_Data/Template.jpg       →  grasp_final/assets/
      Test_Data/*.ged              →  grasp_final/gedcom/
    """
    if not paths:
        return

    _print("\nSyncing project files to configured destinations...")

    # Folder copies
    _copy_folder_contents(paths.get('media_source'),          paths.get('media'),          "Media")
    _copy_folder_contents(paths.get('special_images_source'), paths.get('special_images'), "Special images")
    _copy_folder_contents(paths.get('styles_source'),         paths.get('styles'),       "Styles")
    _copy_single_file(paths.get('results_list_source'),       paths.get('results_list'), "ResultsList.plist")

    # Asset files: logo.gif and Template.jpg from the assets_source folder
    assets_src = paths.get('assets_source', '').strip()
    assets_dest = paths.get('assets', '').strip()
    for asset_file in ('logo.gif', 'Template.jpg', 'background1.jpg'):
        src = os.path.join(assets_src, asset_file) if assets_src else ''
        _copy_single_file(src, assets_dest, asset_file)

    # GEDCOM files: copy all .ged files from gedcom_source folder to gedcom dest
    gedcom_src  = paths.get('gedcom_source', '').strip()
    gedcom_dest = paths.get('gedcom', '').strip()
    if gedcom_src and gedcom_dest:
        if not os.path.isdir(gedcom_src):
            print(f"  WARNING: GEDCOM source folder not found: {gedcom_src}", file=sys.stderr)
        elif not os.path.isdir(gedcom_dest):
            print(f"  WARNING: GEDCOM destination folder not found: {gedcom_dest}", file=sys.stderr)
        elif os.path.abspath(gedcom_src) == os.path.abspath(gedcom_dest):
            _print(f"  GEDCOM: source and destination are the same folder, skipping.")
        else:
            copied = overwritten = 0
            for fname in os.listdir(gedcom_src):
                if not fname.lower().endswith('.ged'):
                    continue
                src_file  = os.path.join(gedcom_src,  fname)
                dest_file = os.path.join(gedcom_dest, fname)
                if os.path.exists(dest_file):
                    overwritten += 1
                shutil.copy2(src_file, dest_file)
                copied += 1
            _print(f"  GEDCOM: {copied} .ged file(s) copied ({overwritten} overwritten)  →  {gedcom_dest}")

    # Config file: website_config.ini → grasp_final/config/
    _copy_single_file(paths.get('config_source'), paths.get('config'), "website_config.ini")

    _print("  Sync complete.")


def copy_ged_to_gedcom_folder(output_path, paths):
    """Copy the finished .ged to the configured Gedcom destination folder."""
    gedcom_folder = paths.get('gedcom', '').strip()
    if not gedcom_folder:
        return
    if not os.path.isdir(gedcom_folder):
        print(f"  WARNING: Gedcom destination folder not found: {gedcom_folder}", file=sys.stderr)
        _print(f"           (Gedcom= must point to a folder, not a .ged file)")
        return
    dest    = os.path.join(gedcom_folder, os.path.basename(output_path))
    if os.path.abspath(output_path) == os.path.abspath(dest):
        _print(f"  GEDCOM: output is already in the destination folder, skipping copy.")
        return
    existed = os.path.exists(dest)
    shutil.copy2(output_path, dest)
    _print(f"  GEDCOM copied{' (overwritten)' if existed else ''}  →  {dest}")


# ===========================================================================
# ARGUMENT PARSER  (single-tool subcommands kept for backward compatibility)
# ===========================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="gedcom_cleanup.py",
        description="Consolidated GEDCOM cleanup tool.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
━━━  MULTI-TOOL MODE  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  detect  --input FILE  [--report_citations CSV] [--report_publ CSV]
                        [--report_resi_dates CSV] [--report_media CSV]
                        [--report_photo CSV] [--report_mojibake CSV]
                        [--report_dates CSV] [--report_sex CSV]
                        [--report_file_paths CSV] [--quiet]

  apply   --input FILE  --output FILE
                        [--report_citations CSV] [--report_publ CSV]
                        [--report_resi_dates CSV] [--report_media CSV]
                        [--report_mojibake CSV] [--report_dates CSV]
                        [--report_sex CSV] [--report_file_paths CSV]

  Tools run in order: citations → publ → resi_dates → media →
                      mojibake → dates → sex → file_paths.
  Omit a --report_* flag to skip that tool entirely.
  In apply mode each tool's output feeds the next automatically.

Examples (multi-tool):
  python gedcom_cleanup.py detect --input 426GEN6.ged \\
      --report_citations citations.csv --report_publ publ.csv \\
      --report_resi_dates resi.csv --report_media media.csv --report_photo photo.csv

  python gedcom_cleanup.py apply --input 426GEN6.ged --output 426GEN6_clean.ged \\
      --report_citations citations.csv --report_publ publ.csv \\
      --report_resi_dates resi.csv --report_media media.csv

━━━  SINGLE-TOOL MODE (original, still supported)  ━━━━━━━━━━━━━━━━━━━━━━━━━

  citations   detect  --input FILE --report CSV [--quiet]
  citations   apply   --input FILE --report CSV --output FILE
  publ        detect  --input FILE --report CSV
  publ        apply   --input FILE --report CSV --output FILE
  resi_dates  detect  --input FILE --report CSV
  resi_dates  apply   --input FILE --report CSV --output FILE
  media       detect  --input FILE --report CSV
  media       apply   --input FILE --report CSV --output FILE
  nophoto     detect  --input FILE --report CSV
        """
    )

    parser.add_argument(
        "--config", default=None, metavar="INI",
        help="Path to website_config.ini (default: ../config/website_config.ini relative to script)"
    )

    sub = parser.add_subparsers(dest="tool", required=True, metavar="COMMAND")

    # ── multi-tool DETECT ────────────────────────────────────────────────────
    p_detect = sub.add_parser("detect", help="Run one or more detect tools in a single pass")
    p_detect.add_argument("--input",             required=True, help="Input GEDCOM file (.ged)")
    p_detect.add_argument("--report_citations",  default=None,  help="CSV report for duplicate citations")
    p_detect.add_argument("--report_publ",       default=None,  help="CSV report for PUBL corruption")
    p_detect.add_argument("--report_resi_dates", default=None,  help="CSV report for missing RESI dates")
    p_detect.add_argument("--report_media",      default=None,  help="CSV report for duplicate media")
    p_detect.add_argument("--report_photo",      default=None,  help="CSV report for individuals without _PHOTO")
    p_detect.add_argument("--report_mojibake",   default=None,  help="CSV report for mojibake encoding artifacts")
    p_detect.add_argument("--report_dates",      default=None,  help="CSV report for numeric DATE values")
    p_detect.add_argument("--report_sex",        default=None,  help="CSV report for individuals missing SEX tag")
    p_detect.add_argument("--report_file_paths", default=None,  help="CSV report for non-standard FILE paths")
    p_detect.add_argument("--date_style",        default="us", choices=["us","dmy"],
                          help="Numeric date style: us=MM/DD/YYYY  dmy=DD/MM/YYYY (default: us)")
    p_detect.add_argument("--media_subdir",      default="media",
                          help="Expected media subfolder name for file_paths check (default: media)")
    p_detect.add_argument("--quiet",             action="store_true", help="Skip detailed citation summary")

    # ── multi-tool APPLY ─────────────────────────────────────────────────────
    p_apply = sub.add_parser("apply", help="Run one or more apply tools in sequence")
    p_apply.add_argument("--input",             required=True, help="Input GEDCOM file (.ged)")
    p_apply.add_argument("--output",            required=True, help="Final cleaned GEDCOM file (.ged)")
    p_apply.add_argument("--report_citations",  default=None,  help="Reviewed citations CSV")
    p_apply.add_argument("--report_publ",       default=None,  help="Reviewed PUBL CSV")
    p_apply.add_argument("--report_resi_dates", default=None,  help="Reviewed resi_dates CSV")
    p_apply.add_argument("--report_media",      default=None,  help="Reviewed media CSV")
    p_apply.add_argument("--report_mojibake",   default=None,  help="Reviewed mojibake CSV")
    p_apply.add_argument("--report_dates",      default=None,  help="Reviewed dates CSV")
    p_apply.add_argument("--report_sex",        default=None,  help="Reviewed sex CSV")
    p_apply.add_argument("--report_file_paths", default=None,  help="Reviewed file_paths CSV")
    p_apply.add_argument("--quiet",             action="store_true", help="Suppress progress output; errors go to stderr")

    # ── single-tool subcommands (backward-compatible) ─────────────────────────
    def add_single_tool(name, help_text):
        t  = sub.add_parser(name, help=help_text)
        ps = t.add_subparsers(dest="phase", required=True, metavar="PHASE")
        pd = ps.add_parser("detect", help="Phase 1: scan and report issues")
        pd.add_argument("--input",  required=True, help="Input GEDCOM file (.ged)")
        pd.add_argument("--report", required=True, help="Output CSV report path")
        if name == "citations":
            pd.add_argument("--quiet", action="store_true", help="Skip detailed summary")
        if name != "nophoto":
            pa = ps.add_parser("apply", help="Phase 2: apply fixes from reviewed CSV")
            pa.add_argument("--input",  required=True, help="Input GEDCOM file (.ged)")
            pa.add_argument("--report", required=True, help="Reviewed CSV path")
            pa.add_argument("--output", required=True, help="Output cleaned GEDCOM file (.ged)")
            pa.add_argument("--quiet",  action="store_true", help="Suppress progress output; errors go to stderr")
        return t

    add_single_tool("citations",  "Consolidate duplicate SOUR citation blocks")
    add_single_tool("publ",       "Clean corrupted PUBL tags (Ancestry exports)")
    add_single_tool("resi_dates", "Infer missing DATE values for RESI events")
    add_single_tool("media",      "Remove duplicate OBJE media attachments")
    add_single_tool("nophoto",    "List INDI records with no _PHOTO tag (detect only)")

    # missing_media is detect-only but needs an extra --media_dir argument
    t_mm = sub.add_parser("missing_media", help="List FILE references not found in the media folder (detect only)")
    ps_mm = t_mm.add_subparsers(dest="phase", required=True, metavar="PHASE")
    pd_mm = ps_mm.add_parser("detect", help="Scan GEDCOM FILE tags and report missing files")
    pd_mm.add_argument("--input",     required=True, help="Input GEDCOM file (.ged)")
    pd_mm.add_argument("--report",    required=True, help="Output CSV report path")
    pd_mm.add_argument("--media_dir", required=True, help="Path to the media folder to check against")
    add_single_tool("mojibake",   "Fix UTF-8 mojibake encoding artifacts")
    add_single_tool("dates",      "Normalize numeric DATE values to GEDCOM standard")
    add_single_tool("sex",        "Insert SEX U for individuals missing a SEX tag")
    add_single_tool("file_paths", "Rewrite FILE paths to canonical media/ subdir")

    return parser


# ── single-tool dispatch table ────────────────────────────────────────────────
SINGLE_TOOL_DISPATCH = {
    ("citations",  "detect"): cmd_citations_detect,
    ("citations",  "apply"):  cmd_citations_apply,
    ("publ",       "detect"): cmd_publ_detect,
    ("publ",       "apply"):  cmd_publ_apply,
    ("resi_dates", "detect"): cmd_resi_dates_detect,
    ("resi_dates", "apply"):  cmd_resi_dates_apply,
    ("media",      "detect"): cmd_media_detect,
    ("media",      "apply"):  cmd_media_apply,
    ("nophoto",       "detect"): cmd_nophoto_detect,
    ("missing_media", "detect"): cmd_missing_media_detect,
    ("mojibake",   "detect"): cmd_mojibake_detect,
    ("mojibake",   "apply"):  cmd_mojibake_apply,
    ("dates",      "detect"): cmd_dates_detect,
    ("dates",      "apply"):  cmd_dates_apply,
    ("sex",        "detect"): cmd_sex_detect,
    ("sex",        "apply"):  cmd_sex_apply,
    ("file_paths", "detect"): cmd_file_paths_detect,
    ("file_paths", "apply"):  cmd_file_paths_apply,
}


# ── multi-tool DETECT ─────────────────────────────────────────────────────────
def cmd_multi_detect(args, paths):
    """Run all requested detect tools in a single pass."""
    require_files(args.input)
    ran_any = False

    if args.report_citations:
        _print(f"\n=== CITATIONS: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_citations}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = citations_detect(lines)
        if not rows:
            _print("  No duplicate source citations found.")
        else:
            citations_write_report(rows, args.report_citations)
            if not args.quiet:
                citations_print_summary(rows)
        ran_any = True

    if args.report_publ:
        _print(f"\n=== PUBL: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_publ}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        publ_detect(lines, args.report_publ)
        ran_any = True

    if args.report_resi_dates:
        _print(f"\n=== RESI_DATES: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_resi_dates}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        resi_detect(lines, args.report_resi_dates)
        ran_any = True

    if args.report_media:
        _print(f"\n=== MEDIA: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_media}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = media_detect(lines)
        if not rows:
            _print("  No duplicate media attachments found.")
        else:
            media_write_report(rows, args.report_media)
        ran_any = True

    if args.report_photo:
        _print(f"\n=== NOPHOTO: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_photo}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = nophoto_detect(lines)
        if not rows:
            _print("  All individuals have a _PHOTO tag.")
        else:
            nophoto_write_report(rows, args.report_photo)
        ran_any = True

    if args.report_mojibake:
        _print(f"\n=== MOJIBAKE: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_mojibake}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = mojibake_detect(lines, args.report_mojibake)
        if not rows:
            _print("  No mojibake artifacts found.")
        ran_any = True

    if args.report_dates:
        _print(f"\n=== DATES: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_dates}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = dates_detect(lines, args.report_dates, getattr(args, 'date_style', 'us'))
        if not rows:
            _print("  No numeric dates found.")
        ran_any = True

    if args.report_sex:
        _print(f"\n=== SEX: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_sex}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = sex_detect(lines, args.report_sex)
        if not rows:
            _print("  All individuals have a SEX tag.")
        ran_any = True

    if args.report_file_paths:
        _print(f"\n=== FILE_PATHS: DETECT ===")
        _print(f"  Input : {args.input}")
        _print(f"  Report: {args.report_file_paths}")
        lines = read_gedcom_lines(args.input)
        _print(f"  Read {len(lines):,} lines.  Scanning...")
        rows = file_paths_detect(lines, args.report_file_paths, getattr(args, 'media_subdir', 'media'))
        if not rows:
            _print("  All FILE paths already use the correct media subdir.")
        ran_any = True

    if not ran_any:
        _print("  No --report_* flags supplied — nothing to detect.")
        _print("  Specify at least one of: --report_citations  --report_publ")
        _print("                           --report_resi_dates --report_media  --report_photo")


# ── multi-tool APPLY ──────────────────────────────────────────────────────────
def cmd_multi_apply(args, paths):
    """
    Run requested apply tools in order: citations → publ → resi_dates → media.
    Each tool's output feeds the next via a temp file.
    """
    require_files(args.input)

    # If --output is a directory, derive output filename from input stem + _1
    if os.path.isdir(args.output):
        base = os.path.basename(args.input)
        stem, ext = os.path.splitext(base)
        args.output = os.path.join(args.output, stem + "_1" + (ext or ".ged"))
        _print(f"  Output is a directory — writing to: {args.output}")

    require_distinct(args.input, args.output)

    tools  = [
        ("citations",  args.report_citations),
        ("publ",       args.report_publ),
        ("resi_dates", args.report_resi_dates),
        ("media",      args.report_media),
        ("mojibake",   args.report_mojibake),
        ("dates",      args.report_dates),
        ("sex",        args.report_sex),
        ("file_paths", args.report_file_paths),
    ]
    active = [(name, report) for name, report in tools if report]

    if not active:
        _print("  No --report_* flags supplied — nothing to apply.")
        return

    for name, report in active:
        if not os.path.isfile(report):
            sys.exit(f"ERROR: Report file not found for {name}: {report}")

    current_input = args.input
    temp_files    = []

    try:
        for i, (name, report) in enumerate(active):
            is_last = (i == len(active) - 1)
            if is_last:
                current_output = args.output
            else:
                tmp = tempfile.NamedTemporaryFile(
                    suffix=f"_gedcom_{name}.ged", delete=False)
                tmp.close()
                current_output = tmp.name
                temp_files.append(current_output)

            _print(f"\n=== {name.upper()}: APPLY ===")
            _print(f"  Input : {current_input}")
            _print(f"  Report: {report}")
            _print(f"  Output: {current_output}")
            lines = read_gedcom_lines(current_input)
            _print(f"  Read {len(lines):,} lines.  Applying fixes...")

            if name == "citations":
                citations_apply(lines, report, current_output)
            elif name == "publ":
                publ_apply(lines, report, current_output)
            elif name == "resi_dates":
                resi_apply(lines, report, current_output)
            elif name == "media":
                media_apply(lines, report, current_output)
            elif name == "mojibake":
                mojibake_apply(lines, report, current_output)
            elif name == "dates":
                dates_apply(lines, report, current_output)
            elif name == "sex":
                sex_apply(lines, report, current_output)
            elif name == "file_paths":
                file_paths_apply(lines, report, current_output)

            current_input = current_output

        _print(f"\n=== APPLY COMPLETE ===")
        _print(f"  Final output: {args.output}")
        copy_ged_to_gedcom_folder(args.output, paths)

    finally:
        for tmp in temp_files:
            try:
                os.remove(tmp)
            except OSError:
                pass


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = build_parser()
    args   = parser.parse_args()

    global _quiet
    _quiet = getattr(args, 'quiet', False)

    paths = load_project_paths(config_path=getattr(args, 'config', None))
    print_project_paths(paths)
    sync_project_files(paths)
    _print(args, paths)
    tool = args.tool
    if tool == "detect":
        cmd_multi_detect(args, paths)
    elif tool == "apply":
        cmd_multi_apply(args, paths)
    else:
        phase = getattr(args, 'phase', None)
        fn    = SINGLE_TOOL_DISPATCH.get((tool, phase))
        if fn:
            fn(args)
            if phase == "apply" and hasattr(args, "output"):
                copy_ged_to_gedcom_folder(args.output, paths)
        else:
            parser.print_help()
            sys.exit(1)


if __name__ == "__main__":
    main()
