#!/usr/bin/env python3
"""
gedcom_prune.py — Tailor a GEDCOM by removing selected individuals and cleaning
up the records that supported only them.

Companion to gedcom_cleanup.py; reuses the same line-based parsing model
(read_gedcom_lines / is_pointer / get_block_lines).

WHAT IT DOES (surgical mode):
  * Removes the individuals you list (by GEDCOM id, e.g. @I327@).
  * Detaches them from every family (drops the HUSB/WIFE/CHIL line that pointed
    at them) and removes any other dangling pointer to them (e.g. ASSO).
  * Deletes a family only if NOBODY is left in it after detaching. Spouses,
    children, and parents of a removed person are kept.
  * Optionally prunes source (SOUR) and media (OBJE) records that are no longer
    referenced by anything that remains (on by default; cascades — removing a
    source can orphan its media, which is then pruned too).
  * Writes a new GEDCOM and prints a report. Never touches the input file.

USAGE:
  # 1) Preview first (writes nothing, shows who/what would go):
  python3 gedcom_prune.py INPUT.ged --remove ids.txt --output OUT.ged --dry-run

  # 2) Apply for real:
  python3 gedcom_prune.py INPUT.ged --remove ids.txt --output OUT.ged

  --remove FILE     text/CSV file: one individual id per line (@I327@ or I327).
                    Blank lines and lines starting with # are ignored; if a line
                    has commas, only the first field is read (so a CSV works).
  --keep-orphans    do NOT prune unreferenced sources/media (keep everything).
  --keep-orphan-sources   prune orphaned media only, keep all sources.
  --dry-run         report the plan but write no output file.
  --report FILE     also write a CSV report of everything removed.
"""

import os
import sys
import argparse
import csv

# --------------------------------------------------------------------------
# Shared GEDCOM utilities (mirrors gedcom_cleanup.py)
# --------------------------------------------------------------------------

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
    return isinstance(value, str) and value.startswith("@") and value.endswith("@")


def get_block_lines(gedcom_lines, start_idx):
    """Return line indices forming the block at start_idx (it + deeper lines)."""
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
    for p in paths:
        if not os.path.isfile(p):
            sys.exit(f"ERROR: File not found: {p}")


def require_distinct(input_path, output_path):
    if os.path.abspath(input_path) == os.path.abspath(output_path):
        sys.exit("ERROR: Output file must differ from input file.")


# --------------------------------------------------------------------------
# Record model
# --------------------------------------------------------------------------

def index_records(lines):
    """Map each top-level record's xref id -> {type, start, block:set(indices)}.

    Only level-0 lines whose tag is a pointer (e.g. '0 @I1@ INDI') are records.
    """
    records = {}
    n = len(lines)
    for i in range(n):
        level, tag, rest, _ = lines[i]
        if level == 0 and is_pointer(tag):
            block = get_block_lines(lines, i)
            records[tag] = {
                "type": rest.strip(),      # INDI / FAM / SOUR / OBJE / REPO ...
                "start": i,
                "block": set(block),
            }
    return records


def first_subvalue(lines, rec, subtag):
    """Return the 'rest' of the first level-1 line with the given tag, or ''."""
    start = rec["start"]
    for idx in sorted(rec["block"]):
        if idx == start:
            continue
        level, tag, rest, _ = lines[idx]
        if level == 1 and tag == subtag:
            return rest
    return ""


def person_name(lines, rec):
    """Human-readable name from an INDI record's first NAME line."""
    raw = first_subvalue(lines, rec, "NAME")
    if not raw:
        return "(no name)"
    return " ".join(raw.replace("/", " ").split())


def source_label(lines, rec):
    return first_subvalue(lines, rec, "TITL") or first_subvalue(lines, rec, "ABBR") or ""


def media_label(lines, rec):
    return first_subvalue(lines, rec, "FILE") or first_subvalue(lines, rec, "TITL") or ""


# --------------------------------------------------------------------------
# Removal engine
# --------------------------------------------------------------------------

def classify_tokens(tokens):
    """Split a list of removal tokens into (record_ids, event_tags, even_types).

    Tokens may be:
      * a record id:  @I327@ / I327 / @S45@ / @M1006@
      * an event tag: 'EVENT RESI' or 'EVENT:RESI'   -> strip all RESI events
      * an EVEN type: 'EVENTTYPE Arrival' or 'EVENTTYPE:Arrival'
                                                      -> strip EVEN whose TYPE matches
    """
    record_ids = []
    event_tags = set()
    even_types = set()
    for raw in tokens:
        s = str(raw).strip()
        if not s:
            continue
        up = s.upper()
        if up.startswith("EVENTTYPE"):
            val = s[len("EVENTTYPE"):].lstrip(": ").strip()
            if val:
                even_types.add(val)
        elif up.startswith("EVENT:") or up.startswith("EVENT "):
            rest = s[len("EVENT"):].lstrip(": ").strip()
            tag = rest.split()[0].upper() if rest else ""
            if tag:
                event_tags.add(tag)
        else:
            token = s.replace(",", " ").split()[0]   # first field; rest is a note
            if not token:
                continue
            if not token.startswith("@"):
                token = "@" + token
            if not token.endswith("@"):
                token = token + "@"
            record_ids.append(token)
    seen = set()
    out = []
    for i in record_ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out, event_tags, even_types


def load_remove_ids(path):
    """Read removal tokens from a file -> (record_ids, event_tags, even_types)."""
    tokens = []
    with open(path, encoding="utf-8-sig", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            tokens.append(line)
    return classify_tokens(tokens)


def even_type_value(lines, even_idx):
    """Return the TYPE value under a '1 EVEN' block, or None."""
    for k in get_block_lines(lines, even_idx):
        lv, tag, rest, _ = lines[k]
        if lv == 2 and tag == "TYPE":
            return rest
    return None


def referenced_xrefs(lines, alive):
    """Set of all xref ids referenced by any surviving level>=1 pointer line."""
    refs = set()
    for idx, (level, tag, rest, _) in enumerate(lines):
        if not alive[idx]:
            continue
        if level is not None and level >= 1 and is_pointer(rest):
            refs.add(rest)
    return refs


def kill_line_and_subblock(lines, alive, idx):
    for k in get_block_lines(lines, idx):
        alive[k] = False


def prune(lines, remove_ids, prune_sources=True, prune_media=True,
          remove_event_tags=None, remove_even_types=None):
    """Return (alive[], stats dict). Does not write anything."""
    n = len(lines)
    alive = [True] * n
    records = index_records(lines)
    remove_event_tags = {t.upper() for t in (remove_event_tags or set())}
    remove_even_types = set(remove_even_types or set())

    stats = {
        "removed_individuals": [],   # (id, name)
        "removed_sources": [],       # (id, label) explicitly selected
        "removed_media": [],         # (id, label) explicitly selected
        "missing_ids": [],           # ids not found
        "wrong_type_ids": [],        # ids found but not INDI/SOUR/OBJE
        "families_deleted": [],      # (id)
        "families_modified": set(),  # ids that lost a member but survived
        "sources_pruned": [],        # (id, label)
        "media_pruned": [],          # (id, label)
        "events_removed": 0,         # event sub-blocks stripped
        "events_by_kind": {},        # tag or EVEN:type -> count
        "dangling_refs_removed": 0,  # pointer lines removed referencing gone recs
    }

    # --- 1. Resolve the removal set to INDI / SOUR / OBJE records ----------
    remove_indi = set()      # individuals (drive family cleanup)
    remove_rec = set()       # every record to delete (indi + sour + obje)
    for rid in remove_ids:
        rec = records.get(rid)
        if rec is None:
            stats["missing_ids"].append(rid)
            continue
        t = rec["type"]
        if t == "INDI":
            remove_indi.add(rid)
            remove_rec.add(rid)
            stats["removed_individuals"].append((rid, person_name(lines, rec)))
        elif t == "SOUR":
            remove_rec.add(rid)
            stats["removed_sources"].append((rid, source_label(lines, rec)))
        elif t == "OBJE":
            remove_rec.add(rid)
            stats["removed_media"].append((rid, media_label(lines, rec)))
        else:
            stats["wrong_type_ids"].append((rid, t))

    # Kill the selected records outright
    for rid in remove_rec:
        for k in records[rid]["block"]:
            alive[k] = False

    # --- 2. Strip any surviving pointer line that references a removed record
    #        (detaches families HUSB/WIFE/CHIL/ASSO, source citations, and
    #         media links) ----------------------------------------------------
    for idx in range(n):
        if not alive[idx]:
            continue
        level, tag, rest, _ = lines[idx]
        if level is not None and level >= 1 and is_pointer(rest) and rest in remove_rec:
            kill_line_and_subblock(lines, alive, idx)
            stats["dangling_refs_removed"] += 1
            # note which family lost a member (only for individual removals)
            if rest in remove_indi:
                owner = _owning_record(lines, idx)
                if owner and records.get(owner, {}).get("type") == "FAM":
                    stats["families_modified"].add(owner)

    # --- 3. Delete families with nobody left; cascade FAMC/FAMS cleanup -----
    removed_fam = set()
    for xref, rec in records.items():
        if rec["type"] != "FAM":
            continue
        if not any(alive[k] for k in rec["block"]):
            continue  # already fully gone somehow
        has_member = False
        for idx in rec["block"]:
            if not alive[idx]:
                continue
            level, tag, rest, _ = lines[idx]
            if level == 1 and tag in ("HUSB", "WIFE", "CHIL") and is_pointer(rest):
                has_member = True
                break
        if not has_member:
            for k in rec["block"]:
                alive[k] = False
            removed_fam.add(xref)
            stats["families_deleted"].append(xref)

    stats["families_modified"] -= removed_fam  # deleted trumps modified

    # Remove pointers (FAMS/FAMC/etc.) to deleted families
    if removed_fam:
        for idx in range(n):
            if not alive[idx]:
                continue
            level, tag, rest, _ = lines[idx]
            if level is not None and level >= 1 and is_pointer(rest) and rest in removed_fam:
                kill_line_and_subblock(lines, alive, idx)

    # --- 3b. Strip selected event types from surviving INDI / FAM records --
    if remove_event_tags or remove_even_types:
        by_kind = {}
        for xref, rec in records.items():
            if rec["type"] not in ("INDI", "FAM"):
                continue
            for idx in sorted(rec["block"]):
                if not alive[idx]:
                    continue
                level, tag, rest, _ = lines[idx]
                if level != 1:
                    continue
                kind = None
                if tag in remove_event_tags:
                    kind = tag
                elif tag == "EVEN" and remove_even_types:
                    tv = even_type_value(lines, idx)
                    if tv in remove_even_types:
                        kind = f"EVEN:{tv}"
                if kind:
                    kill_line_and_subblock(lines, alive, idx)
                    stats["events_removed"] += 1
                    by_kind[kind] = by_kind.get(kind, 0) + 1
        stats["events_by_kind"] = by_kind

    # --- 4. Prune orphaned SOUR / OBJE, iterating to convergence -----------
    if prune_sources or prune_media:
        want_types = set()
        if prune_sources:
            want_types.add("SOUR")
        if prune_media:
            want_types.add("OBJE")
        while True:
            refs = referenced_xrefs(lines, alive)
            newly = []
            for xref, rec in records.items():
                if rec["type"] not in want_types:
                    continue
                if not any(alive[k] for k in rec["block"]):
                    continue  # already removed
                if xref not in refs:
                    newly.append(xref)
            if not newly:
                break
            for xref in newly:
                rec = records[xref]
                for k in rec["block"]:
                    alive[k] = False
                if rec["type"] == "SOUR":
                    stats["sources_pruned"].append((xref, source_label(lines, rec)))
                else:
                    stats["media_pruned"].append((xref, media_label(lines, rec)))

    return alive, stats


def _owning_record(lines, idx):
    """Walk upward to find the enclosing level-0 record's xref id."""
    j = idx
    while j >= 0:
        level, tag, rest, _ = lines[j]
        if level == 0 and is_pointer(tag):
            return tag
        j -= 1
    return None


# --------------------------------------------------------------------------
# Output + reporting
# --------------------------------------------------------------------------

def write_gedcom(lines, alive, output_path):
    with open(output_path, "w", encoding="utf-8") as fh:
        for idx, (level, tag, rest, raw) in enumerate(lines):
            if alive[idx]:
                fh.write(raw + "\n")


def print_report(stats, dry_run):
    print()
    print("=" * 60)
    print("  GEDCOM PRUNE " + ("(DRY RUN — nothing written)" if dry_run else "REPORT"))
    print("=" * 60)

    if stats["missing_ids"]:
        print(f"\n  !! {len(stats['missing_ids'])} id(s) NOT FOUND (skipped):")
        for rid in stats["missing_ids"]:
            print(f"       {rid}")
    if stats["wrong_type_ids"]:
        print(f"\n  !! {len(stats['wrong_type_ids'])} id(s) are not removable (skipped):")
        for rid, typ in stats["wrong_type_ids"]:
            print(f"       {rid} is a {typ}")

    print(f"\n  Individuals removed: {len(stats['removed_individuals'])}")
    for rid, name in stats["removed_individuals"]:
        print(f"       {rid}  {name}")

    if stats["removed_sources"]:
        print(f"\n  Sources removed (selected): {len(stats['removed_sources'])}")
        for rid, label in stats["removed_sources"]:
            print(f"       {rid}  {label}")
    if stats["removed_media"]:
        print(f"\n  Media removed (selected): {len(stats['removed_media'])}")
        for rid, label in stats["removed_media"]:
            print(f"       {rid}  {label}")

    print(f"\n  Families deleted (emptied): {len(stats['families_deleted'])}")
    for xref in stats["families_deleted"]:
        print(f"       {xref}")
    print(f"  Families kept but detached from a removed member: {len(stats['families_modified'])}")

    if stats["events_removed"]:
        print(f"\n  Events removed: {stats['events_removed']}")
        for kind, cnt in sorted(stats["events_by_kind"].items()):
            print(f"       {kind}: {cnt}")

    print(f"\n  Sources pruned (orphaned): {len(stats['sources_pruned'])}")
    print(f"  Media pruned (orphaned):   {len(stats['media_pruned'])}")
    print(f"  Stray pointer lines removed: {stats['dangling_refs_removed']}")
    print("=" * 60)


def write_csv_report(stats, path):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["action", "type", "id", "detail"])
        for rid, name in stats["removed_individuals"]:
            w.writerow(["removed", "INDI", rid, name])
        for rid, label in stats["removed_sources"]:
            w.writerow(["removed", "SOUR", rid, label])
        for rid, label in stats["removed_media"]:
            w.writerow(["removed", "OBJE", rid, label])
        for xref in stats["families_deleted"]:
            w.writerow(["deleted", "FAM", xref, "emptied"])
        for xref in sorted(stats["families_modified"]):
            w.writerow(["modified", "FAM", xref, "detached removed member"])
        for xref, label in stats["sources_pruned"]:
            w.writerow(["pruned", "SOUR", xref, label])
        for xref, label in stats["media_pruned"]:
            w.writerow(["pruned", "OBJE", xref, label])
        for kind, cnt in sorted(stats.get("events_by_kind", {}).items()):
            w.writerow(["removed", "EVENT", kind, f"{cnt} stripped"])
        for rid in stats["missing_ids"]:
            w.writerow(["skipped", "?", rid, "id not found"])
        for rid, typ in stats["wrong_type_ids"]:
            w.writerow(["skipped", typ, rid, "not removable (INDI/SOUR/OBJE only)"])


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Remove selected individuals from a GEDCOM and clean up "
                    "their supporting records.")
    ap.add_argument("input", help="input GEDCOM file")
    ap.add_argument("--remove", required=True,
                    help="file listing ids to remove (individuals/sources/media), "
                         "and/or event lines like 'EVENT RESI' or 'EVENTTYPE Arrival'")
    ap.add_argument("--output", required=True, help="output GEDCOM file")
    ap.add_argument("--remove-events", default="",
                    help="comma-separated event tags to strip, e.g. RESI,BAPM")
    ap.add_argument("--keep-orphans", action="store_true",
                    help="keep ALL sources and media even if unreferenced")
    ap.add_argument("--keep-orphan-sources", action="store_true",
                    help="prune orphaned media only; keep all sources")
    ap.add_argument("--dry-run", action="store_true",
                    help="report the plan but do not write the output file")
    ap.add_argument("--report", help="also write a CSV report to this path")
    args = ap.parse_args()

    require_files(args.input, args.remove)
    require_distinct(args.input, args.output)

    prune_sources = not (args.keep_orphans or args.keep_orphan_sources)
    prune_media = not args.keep_orphans

    lines = read_gedcom_lines(args.input)
    remove_ids, event_tags, even_types = load_remove_ids(args.remove)
    if args.remove_events:
        event_tags |= {t.strip().upper() for t in args.remove_events.split(",") if t.strip()}
    if not (remove_ids or event_tags or even_types):
        sys.exit("ERROR: nothing to remove (no ids or event types found).")

    alive, stats = prune(lines, remove_ids,
                         prune_sources=prune_sources, prune_media=prune_media,
                         remove_event_tags=event_tags, remove_even_types=even_types)

    print_report(stats, args.dry_run)

    if args.report:
        write_csv_report(stats, args.report)
        print(f"\n  CSV report written: {args.report}")

    if args.dry_run:
        print("\n  Dry run — no output file written. "
              "Re-run without --dry-run to apply.")
        return

    write_gedcom(lines, alive, args.output)
    kept = sum(1 for a in alive if a)
    print(f"\n  Output written: {args.output}  ({kept} lines kept "
          f"of {len(lines)})")


if __name__ == "__main__":
    main()
