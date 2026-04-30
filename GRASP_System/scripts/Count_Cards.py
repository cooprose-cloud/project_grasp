#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
count_cards.py — Count GEDCOM "cards" (lines) by GEDCOM section (record type) and grand total.

Definition used:
- A "section" is the current 0-level record context.
  Examples:
    0 HEAD           => section HEAD (often treated as ENV)
    0 @I1@ INDI      => section INDI
    0 @F1@ FAM       => section FAM
    0 @S1@ SOUR      => section SOUR
    0 @M1@ OBJE      => section OBJE
    0 TRLR           => section TRLR (often treated as ENV)

Counts:
- section_totals: counts ALL lines attributed to each section (including the 0-level line)
- grand_total_lines: all lines in file
- grand_total_parsed: lines successfully parsed as GEDCOM lines
- unparseable_lines: lines that did not match the GEDCOM line regex

Optional:
- --detail prints a breakdown by (Section, Tag, Level)
- --json writes a JSON report
"""

from __future__ import annotations
import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

print('count_cards.py — Count GEDCOM "cards" (lines) by GEDCOM section (record type) and grand total.')

LINE_RE = re.compile(
    r'^(?P<level>\d+)\s+(?:(?P<xref>@[^@]+@)\s+)?(?P<tag>[A-Za-z0-9_]+)(?:\s+(?P<value>.*))?$'
)

@dataclass(frozen=True)
class GedLine:
    level: int
    xref: Optional[str]
    tag: str
    value: str

def parse_line(raw: str) -> Optional[GedLine]:
    m = LINE_RE.match(raw.rstrip("\n\r"))
    if not m:
        return None
    return GedLine(
        level=int(m.group("level")),
        xref=m.group("xref"),
        tag=(m.group("tag") or "").upper(),
        value=(m.group("value") or ""),
    )

def normalize_section(tag0: str) -> str:
    """
    Collapse some 0-level tags into broader 'ENV' if you prefer.
    You can change this behavior easily.

    Current behavior:
    - HEAD and TRLR are counted as ENV
    - Everything else uses its 0-level tag as the section (INDI/FAM/SOUR/OBJE/NOTE/REPO/SUBM/...)
    """
    if tag0 in ("HEAD", "TRLR"):
        return "ENV"
    return tag0 or "ENV"

def build_report(path: Path, *, keep_detail: bool) -> Dict[str, object]:
    section_totals = Counter()          # Section -> total lines attributed
    record_headers = Counter()          # TAG at level 0 with xref -> count (e.g., INDI/FAM/SOUR)
    top_level_nonxref = Counter()       # HEAD/TRLR/etc. without xref -> count

    # Optional detail: (Section, Tag, Level) -> count
    detail = Counter() if keep_detail else None

    grand_total_lines = 0
    grand_total_parsed = 0
    unparseable_lines = 0

    current_section = "ENV"
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            grand_total_lines += 1
            gl = parse_line(raw)
            if gl is None:
                unparseable_lines += 1
                # If you want unparseable lines counted toward current section, uncomment next line:
                # section_totals[current_section] += 1
                continue
            grand_total_parsed += 1

            if gl.level == 0:
                # 0-level line defines section context
                if gl.xref is not None:
                    record_headers[gl.tag] += 1
                else:
                    top_level_nonxref[gl.tag] += 1
                current_section = normalize_section(gl.tag)

            # Count this line into the current section
            section_totals[current_section] += 1

            if keep_detail and detail is not None:
                detail[(current_section, gl.tag, gl.level)] += 1
    report: Dict[str, object] = {
        "file": str(path),
        "grand_total_lines": grand_total_lines,
        "grand_total_parsed": grand_total_parsed,
        "unparseable_lines": unparseable_lines,
        "section_totals": dict(section_totals),
        "record_headers": dict(record_headers),
        "top_level_nonxref": dict(top_level_nonxref),
    }

    if keep_detail and detail is not None:
        # Flatten detail for JSON friendliness
        report["detail"] = [
            {"section": s, "tag": t, "level": lvl, "count": cnt}
            for (s, t, lvl), cnt in sorted(detail.items(), key=lambda x: (x[0][0], x[0][1], x[0][2]))
        ]
    return report

def print_report(report: Dict[str, object], *, show_detail: bool) -> None:
    print()
    print(f"GEDCOM file: {report['file']}")
    print()

    print("Total cards by GEDCOM section:")
    section_totals = report["section_totals"]
    for sec in sorted(section_totals.keys()):
        print(f"  {sec:<6} {section_totals[sec]:>10}")

    print()
    print(f"Grand total lines (all):        {report['grand_total_lines']:>10}")
    print(f"Grand total lines (parsed):     {report['grand_total_parsed']:>10}")
    print(f"Unparseable lines:              {report['unparseable_lines']:>10}")
    print()

    print("Record header counts (0 @X@ TAG):")
    rh = report["record_headers"]
    if rh:
        for k in sorted(rh.keys()):
            print(f"  {k:<6} {rh[k]:>10}")
    else:
        print("  (none detected)")
    print()
    print("Top-level non-xref counts (e.g., HEAD/TRLR):")
    tln = report["top_level_nonxref"]
    if tln:
        for k in sorted(tln.keys()):
            print(f"  {k:<6} {tln[k]:>10}")
    else:
        print("  (none detected)")
    print()

    if not show_detail:
        return

    detail_rows: List[Dict[str, object]] = report.get("detail", [])
    if not detail_rows:
        return
    print("Detail (Section, Tag, Level) counts:")
    print("  Section  Tag     Level      Count")
    print("  ------   ------  -----  ---------")
    for row in detail_rows:
        print(f"  {row['section']:<6}   {row['tag']:<6}  {row['level']:>5}  {row['count']:>9}")
    print()

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Count GEDCOM cards by section and grand total.")
    p.add_argument("gedcom_file", type=Path, help="Path to GEDCOM file (.ged)")
    p.add_argument("--json", dest="json_path", type=Path, default=None, help="Write JSON report to this file")
    p.add_argument("--detail", action="store_true", help="Also print detailed counts by (Section, Tag, Level)")
    return p.parse_args()

def main() -> int:
    args = parse_args()
    path: Path = args.gedcom_file
    if not path.exists():
        print(f"ERROR: file not found: {path}")
        return 2

    report = build_report(path, keep_detail=args.detail)
    print_report(report, show_detail=args.detail)
    if args.json_path is not None:
        args.json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Wrote JSON report to: {args.json_path}")

    return 0

if __name__ == "__main__":
    x = input('Press Enter to continue.')
    print('\n\n ')
    
    raise SystemExit(main())
