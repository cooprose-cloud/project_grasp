#!/usr/bin/env python3
"""
GEDCOM to MySQL Importer - Event-Centric Design 

============================================================
NEW IN V4: Extracts unique places from events and populates places table

Design Philosophy:
- events table is SINGLE SOURCE OF TRUTH for all genealogical facts
- individuals table contains pointers to primary events + cached fields for Phase 2
- All NAME/BIRT/DEAT tags create events with complete data
- First occurrence sets primary pointer and populates cache fields
- Places are extracted from events to create a master places list
"""

print('\n\n\n\n\n####################################################\n')
print('GEDCOM to MySQL Importer - Event-Centric Design ')

import mysql.connector
import sys
import re
import os
import argparse
import configparser
import getpass
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

class GedcomImporter:
    def __init__(self, db_config: Dict[str, Any], *, clear_mode: str = "truncate", chunk_size: int = 5000,
                 validate_post_load: bool = True):
        """Create importer.

        Notes:
        - This constructor does NOT clear tables. Clearing is performed in write_to_database()
          and controlled by the clear_mode parameter.
        - Use clear_mode='none' for incremental / debugging loads.
        """
        self.db_config = db_config
        self.clear_mode = (clear_mode or "truncate").lower()
        self.chunk_size = int(chunk_size) if chunk_size else 5000
        self.validate_post_load = bool(validate_post_load)

        self.conn = mysql.connector.connect(**db_config)
        self.cursor = self.conn.cursor()

        # Custom tag sequence counters (avoid O(n^2) scans)
        self._custom_seq = defaultdict(int)

        # Data storage
        self.header = {}
        self.individuals = {}
        self.families = {}
        self.events = {}
        self.notes = {}
        self.sources = {}
        self.media = {}
        self.places = {}
        self.repositories = {}
        
        # Cross-reference tables
        self.indi_event_xref = []
        self.indi_media_xref = []
        self.indi_note_xref = []
        self.indi_source_xref = []
        self.fam_event_xref = []
        self.fam_media_xref = []
        self.fam_note_xref = []
        self.fam_source_xref = []
        self.event_media_xref = []
        self.event_note_xref = []
        self.event_source_xref = []
        self.citation_media_xref = []  # Media attached to citations (3 OBJE under 2 SOUR)
        self.citation_note_xref = []   # Notes attached to citations
        self.fam_citation_media_xref = []  # Media attached to family-level citations
        self.fam_citation_note_xref = []   # Notes attached to family-level citations
        self.indi_citation_media_xref = []  # Media attached to individual-level citations
        self.indi_citation_note_xref = []   # Notes attached to individual-level citations
        self.source_media_xref = []
        self.source_note_xref = []  # Notes attached to sources
        self.source_repo_xref = []
        self.note_media_xref = []  # Media attached to notes (rare but valid)
        self.place_media_xref = []
        self.repo_note_xref = []
        self.child_family_xref = []
        self.spouse_family_xref = []
        
        # Custom tags (underscore tags)
        self.custom_tags = []
        
        # Counters
        self.event_counter = 1
        self.place_counter = 1
        
        # Track first occurrences for primary event pointers
        self.name_count = defaultdict(int)
        self.birth_count = defaultdict(int)
        self.death_count = defaultdict(int)
        
        # Track duplicates for reporting
        self.duplicate_stats = {
            'indi_media': 0,
            'fam_media': 0,
            'event_media': 0,
            'citation_media': 0
        }
    
    def clear_database(self, mode: Optional[str] = None) -> None:
        """Clear database tables prior to load.

        mode:
          - 'truncate' (default): fast, resets AUTO_INCREMENT; requires FK checks temporarily disabled
          - 'delete'            : preserves AUTO_INCREMENT; still disables FK checks for safety
          - 'none'              : do not clear anything
        """
        mode = (mode or self.clear_mode or "truncate").lower()
        if mode == "none":
            print("Clear mode: none (tables not cleared).")
            return

        if mode not in ("truncate", "delete"):
            raise ValueError(f"Unknown clear mode: {mode}")

        tables = [
            'header',
            'custom_tags',
            'citation_custom_tags',
            'fam_citation_note_xref',
            'fam_citation_media_xref',
            'indi_citation_note_xref',
            'indi_citation_media_xref',
            'citation_note_xref',
            'citation_media_xref',
            'note_media_xref',
            'source_note_xref',
            'source_media_xref',
            'source_repo_xref',
            'fam_note_xref',
            'fam_media_xref',
            'fam_source_xref',
            'indi_source_xref',
            'indi_note_xref',
            'event_source_xref',
            'event_note_xref',
            'event_media_xref',
            'fam_event_xref',
            'spouse_family_xref',
            'child_family_xref',
            'indi_media_xref',
            'indi_event_xref',
            'place_media_xref',
            'repo_note_xref',
            'repositories',
            'media',
            'sources',
            'notes',
            'events',
            'places',
            'families',
            'individuals'
        ]

        stmt = "TRUNCATE TABLE {t}" if mode == "truncate" else "DELETE FROM {t}"

        print(f"Clearing tables via {mode.upper()} (FK checks temporarily disabled)...")
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for t in tables:
                try:
                    self.cursor.execute(stmt.format(t=t))
                except mysql.connector.Error as err:
                    # Continue clearing other tables; surface warning for missing/locked tables.
                    print(f"  Warning: Could not clear {t}: {err}")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.conn.commit()
        except Exception:
            # Ensure FK checks are re-enabled on error where possible
            try:
                self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                self.conn.commit()
            except Exception:
                pass
            raise

        print("Database cleared.\n")
    
    def _next_custom_seq(self, parent_type: str, parent_id: str) -> int:
        """Return the next sequence number for (parent_type, parent_id) custom tags."""
        key = (parent_type, parent_id)
        seq = self._custom_seq[key]
        self._custom_seq[key] += 1
        return seq

    def executemany_chunked(self, sql: str, rows: List[Tuple], *, label: str = "") -> None:
        """Chunked executemany() with optional progress output."""
        if not rows:
            return
        chunk = max(1, int(self.chunk_size))
        total = len(rows)
        for i in range(0, total, chunk):
            part = rows[i:i + chunk]
            self.cursor.executemany(sql, part)
            if label:
                done = min(i + chunk, total)
                print(f"  {label}: {done:,}/{total:,}")

    def parse_name(self, name_value):
        """Parse GEDCOM name format: Given /Surname/ Suffix"""
        # Remove slashes to get surname
        parts = name_value.split('/')
        if len(parts) >= 3:
            given = parts[0].strip()
            surname = parts[1].strip()
            suffix = parts[2].strip()
        elif len(parts) == 2:
            given = parts[0].strip()
            surname = parts[1].strip() if parts[1] else ''
            suffix = ''
        else:
            # No surname delimiters
            name_parts = name_value.strip().split()
            given = name_parts[0] if len(name_parts) > 0 else ''
            surname = name_parts[1] if len(name_parts) > 1 else ''
            suffix = ' '.join(name_parts[2:]) if len(name_parts) > 2 else ''
        
        # Build formatted names
        full_name = f"{given} {surname}".strip()
        if suffix:
            full_name += f", {suffix}"
        
        sort_name = f"{surname}, {given}".strip(', ')
        if suffix:
            sort_name = f"{surname}, {given}, {suffix}".strip(', ')
        
        return {
            'given_name': given,
            'surname': surname,
            'suffix': suffix,
            'full_name': full_name,
            'sort_name': sort_name
        }
    
    def extract_year(self, date_str):
        """Extract 4-digit year from GEDCOM date string"""
        if not date_str:
            return None
        match = re.search(r'\b(\d{4})\b', date_str)
        return int(match.group(1)) if match else None
    
    def parse_coordinates(self, lines, start_index, start_level):
        """Parse MAP/LATI/LONG coordinates after PLAC tag - THE FIX!"""
        latitude = None
        longitude = None
        j = start_index + 1
        
        while j < len(lines):
            line = lines[j].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                j += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                j += 1
                continue
            
            level = int(level)
            
            if level <= start_level:
                break
            
            if level == start_level + 1 and parts[1] == 'MAP':
                k = j + 1
                while k < len(lines):
                    coord_line = lines[k].rstrip('\n\r')
                    coord_parts = coord_line.split(' ', 2)
                    if len(coord_parts) < 2:
                        k += 1
                        continue
                    
                    coord_level = coord_parts[0]
                    if not coord_level.isdigit():
                        k += 1
                        continue
                    
                    coord_level = int(coord_level)
                    
                    if coord_level < start_level + 2:
                        break
                    if coord_level > start_level + 2:
                        k += 1
                        continue
                    
                    coord_tag = coord_parts[1]
                    coord_value = coord_parts[2] if len(coord_parts) > 2 else ''
                    
                    if coord_tag == 'LATI':
                        coord_value = coord_value.strip()
                        if coord_value:
                            direction = coord_value[0]
                            value = coord_value[1:]
                            try:
                                latitude = float(value) if direction == 'N' else -float(value)
                            except ValueError:
                                pass
                    elif coord_tag == 'LONG':
                        coord_value = coord_value.strip()
                        if coord_value:
                            direction = coord_value[0]
                            value = coord_value[1:]
                            try:
                                longitude = float(value) if direction == 'E' else -float(value)
                            except ValueError:
                                pass
                    
                    k += 1
                break
            
            j += 1
        
        return latitude, longitude

    def load_places_csv(self, csv_path: str) -> None:
        """Load the reviewed places CSV produced by gedcom_places_build.py.

        Stores a lookup dict keyed by gedcom_place (the raw place string as it
        appears in the GEDCOM after gedcom_places_apply.py has run, i.e. the
        gedcom_corrected value) so that extract_places_from_events() can enrich
        each place record with db_city, db_county, db_state, db_country.

        Only rows with status S or M are loaded (X = excluded, U = unreviewed).
        """
        import csv as _csv
        self._places_csv_lookup = {}   # gedcom_corrected -> row dict
        loaded = skipped = 0
        try:
            with open(csv_path, newline='', encoding='utf-8-sig') as fh:
                reader = _csv.DictReader(fh)
                for row in reader:
                    status = row.get('status', '').strip().upper()
                    if status not in ('S', 'M'):
                        skipped += 1
                        continue
                    # Key on gedcom_corrected (what's in the GEDCOM after apply)
                    key = row.get('gedcom_corrected', '').strip()
                    if not key:
                        key = row.get('gedcom_place', '').strip()
                    if key:
                        self._places_csv_lookup[key] = row
                        loaded += 1
            print(f"  Places CSV loaded: {loaded} rows  ({skipped} skipped U/X)")
        except FileNotFoundError:
            print(f"  WARNING: Places CSV not found: {csv_path}  (places will lack structured fields)")
        except Exception as exc:
            print(f"  WARNING: Could not read places CSV: {exc}  (places will lack structured fields)")

    def extract_places_from_events(self):
        """
        Extract unique places from events table and populate places dictionary.
        If a places CSV was loaded via load_places_csv(), each place record is
        enriched with db_city, db_county, db_state, db_country.
        """
        print("\nExtracting unique places from events...")
        
        lookup = getattr(self, '_places_csv_lookup', {})
        unique_places = {}
        
        for event_id, event in self.events.items():
            place_name = event.get('event_place')
            if not place_name or place_name.strip() == '':
                continue
            
            place_name = place_name.strip()
            
            if place_name not in unique_places:
                place_id = f"P{self.place_counter}"
                self.place_counter += 1

                csv_row = lookup.get(place_name, {})
                unique_places[place_name] = {
                    'place_id':        place_id,
                    'place_name':      place_name,
                    'place_latitude':  event.get('place_latitude'),
                    'place_longitude': event.get('place_longitude'),
                    'db_city':         csv_row.get('db_city',   '').strip() or None,
                    'db_county':       csv_row.get('db_county', '').strip() or None,
                    'db_state':        csv_row.get('db_state',  '').strip() or None,
                    'db_country':      csv_row.get('db_country','').strip() or None,
                    'note_inline':     None,
                }
            else:
                existing = unique_places[place_name]
                if existing['place_latitude'] is None and event.get('place_latitude') is not None:
                    existing['place_latitude']  = event.get('place_latitude')
                    existing['place_longitude'] = event.get('place_longitude')
        
        self.places = {p['place_id']: p for p in unique_places.values()}
        
        with_structured = sum(1 for p in self.places.values() if p.get('db_country'))
        print(f"  Found {len(self.places)} unique places  ({with_structured} with structured fields)")

    def read_continuation(self, lines, index, level):
        """Read CONC/CONT lines and return combined text"""
        text = []
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                break
            line_level = parts[0]
            if not line_level.isdigit() or int(line_level) <= level:
                break
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            if tag == 'CONC':
                text.append(value)
            elif tag == 'CONT':
                text.append('\n' + value)
            else:
                break
            i += 1
        
        return ''.join(text), i - index - 1
    
    def capture_custom_tags(self, lines, start_index, start_level, parent_type, parent_id):
        """Capture all custom tags (starting with underscore) under current context"""
        i = start_index + 1
        sequence = 0
        
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            
            # Stop if we're back at or below the parent level
            if level <= start_level:
                break
            
            # Only process immediate children (one level down)
            if level == start_level + 1:
                tag = parts[1]
                value = parts[2] if len(parts) > 2 else ''
                
                # Capture if tag starts with underscore (skip tags parsed as events)
                if tag.startswith('_') and tag not in ['_MILT']:
                    # Read any CONC/CONT continuations
                    cont_text, lines_consumed = self.read_continuation(lines, i, level)
                    if cont_text:
                        value = value + cont_text
                    
                    self.custom_tags.append({
                        'parent_type': parent_type,
                        'parent_id': parent_id,
                        'tag_name': tag,
                        'tag_value': value,
                        'tag_level': level,
                        'sequence_num': sequence
                    })
                    sequence += 1
                    i += lines_consumed
            
            i += 1

    def parse_gedcom(self, filename):
        """Parse GEDCOM file and populate data structures"""
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        
        i = 0
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            if not line.strip():
                i += 1
                continue
            
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            
            # Level 0 records
            if level == 0:
                if len(parts) >= 3:
                    record_id = parts[1]
                    record_type = parts[2]
                    
                    if record_type == 'INDI':
                        i = self.parse_individual(lines, i, record_id)
                    elif record_type == 'FAM':
                        i = self.parse_family(lines, i, record_id)
                    elif record_type == 'NOTE':
                        i = self.parse_note(lines, i, record_id)
                    elif record_type == 'SOUR':
                        i = self.parse_source(lines, i, record_id)
                    elif record_type == 'OBJE':
                        i = self.parse_media(lines, i, record_id)
                    elif record_type == 'REPO':
                        i = self.parse_repository(lines, i, record_id)
                    else:
                        i += 1
                elif parts[1] == 'HEAD':
                    i = self.parse_header(lines, i)
                else:
                    i += 1
            else:
                i += 1
        
        print(f"Parsed GEDCOM file: {filename}")
        print(f"  Individuals : {len(self.individuals)}")
        print(f"  Families    : {len(self.families)}")
        print(f"  Events      : {len(self.events)}")
        print(f"  Notes       : {len(self.notes)}")
        print(f"  Sources     : {len(self.sources)}")
        print(f"  Media       : {len(self.media)}")
        print(f"  Repositories: {len(self.repositories)}")
        print(f"  (Places extracted during write phase)")
    
    def parse_header(self, lines, index):
        """
        Parse GEDCOM header and capture all environment information
        
        The header contains critical metadata about the GEDCOM file:
        - GEDCOM version and format
        - Source system (software that created the file)
        - Character encoding
        - Submitter information
        - File creation date
        - Language
        - Copyright information
        """
        header = {
            'gedcom_version': None,
            'gedcom_form': None,
            'source_system': None,
            'source_version': None,
            'source_name': None,
            'source_corporation': None,
            'corp_address': None,
            'corp_phone': None,
            'source_data': None,
            'destination': None,
            'date': None,
            'time': None,
            'submitter_id': None,
            'submission_id': None,
            'file_name': None,
            'copyright': None,
            'language': None,
            'character_set': None,
            'character_set_version': None,
            'note_inline': None
        }
        
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            
            # Stop at next level 0 record
            if level == 0:
                break
            
            # Process level 1 tags
            if level == 1:
                tag = parts[1]
                value = parts[2] if len(parts) > 2 else ''
                
                if tag == 'GEDC':
                    # Parse GEDCOM version info (level 2 sub-tags)
                    j = i + 1
                    while j < len(lines):
                        sub_line = lines[j].rstrip('\n\r')
                        sub_parts = sub_line.split(' ', 2)
                        if len(sub_parts) < 2:
                            j += 1
                            continue
                        sub_level = sub_parts[0]
                        if not sub_level.isdigit() or int(sub_level) <= 1:
                            break
                        if int(sub_level) == 2:
                            sub_tag = sub_parts[1]
                            sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                            if sub_tag == 'VERS':
                                header['gedcom_version'] = sub_value
                            elif sub_tag == 'FORM':
                                header['gedcom_form'] = sub_value
                        j += 1
                    i = j - 1
                
                elif tag == 'SOUR':
                    # Source system information
                    header['source_system'] = value
                    # Parse source sub-tags (level 2)
                    j = i + 1
                    while j < len(lines):
                        sub_line = lines[j].rstrip('\n\r')
                        sub_parts = sub_line.split(' ', 2)
                        if len(sub_parts) < 2:
                            j += 1
                            continue
                        sub_level = sub_parts[0]
                        if not sub_level.isdigit() or int(sub_level) <= 1:
                            break
                        if int(sub_level) == 2:
                            sub_tag = sub_parts[1]
                            sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                            if sub_tag == 'VERS':
                                header['source_version'] = sub_value
                            elif sub_tag == 'NAME':
                                header['source_name'] = sub_value
                            elif sub_tag == 'CORP':
                                header['source_corporation'] = sub_value
                                # Parse level 3 under CORP (ADDR, PHON)
                                k = j
                                while k < len(lines):
                                    corp_line = lines[k].rstrip('\n\r')
                                    corp_parts = corp_line.split(' ', 2)
                                    if len(corp_parts) < 2 or not corp_parts[0].isdigit():
                                        k += 1
                                        continue
                                    corp_level = int(corp_parts[0])
                                    if corp_level <= 2:
                                        break
                                    if corp_level == 3:
                                        corp_tag = corp_parts[1]
                                        corp_value = corp_parts[2] if len(corp_parts) > 2 else ''
                                        if corp_tag == 'ADDR':
                                            header['corp_address'] = corp_value
                                            # Check for CONT lines under ADDR
                                            m = k + 1
                                            while m < len(lines):
                                                addr_line = lines[m].rstrip('\n\r')
                                                addr_parts = addr_line.split(' ', 2)
                                                if len(addr_parts) < 2 or not addr_parts[0].isdigit():
                                                    break
                                                if int(addr_parts[0]) <= 3:
                                                    break
                                                if int(addr_parts[0]) == 4 and addr_parts[1] == 'CONT':
                                                    header['corp_address'] += '\n' + (addr_parts[2] if len(addr_parts) > 2 else '')
                                                m += 1
                                        elif corp_tag == 'PHON':
                                            header['corp_phone'] = corp_value
                                    k += 1
                            elif sub_tag == 'DATA':
                                header['source_data'] = sub_value
                        j += 1
                    i = j - 1
                
                elif tag == 'DEST':
                    header['destination'] = value
                
                elif tag == 'DATE':
                    header['date'] = value
                    # Check for TIME sub-tag
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].rstrip('\n\r')
                        next_parts = next_line.split(' ', 2)
                        if (len(next_parts) >= 2 and next_parts[0] == '2' and 
                            next_parts[1] == 'TIME'):
                            header['time'] = next_parts[2] if len(next_parts) > 2 else ''
                            i += 1
                
                elif tag == 'SUBM':
                    header['submitter_id'] = value
                
                elif tag == 'SUBN':
                    header['submission_id'] = value
                
                elif tag == 'FILE':
                    header['file_name'] = value
                
                elif tag == 'COPR':
                    header['copyright'] = value
                    # Handle CONC/CONT for multi-line copyright
                    copr_cont, skip = self.read_continuation(lines, i, 1)
                    if copr_cont:
                        header['copyright'] += copr_cont
                    i += skip
                
                elif tag == 'LANG':
                    header['language'] = value
                
                elif tag == 'CHAR':
                    header['character_set'] = value
                    # Check for VERS sub-tag
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].rstrip('\n\r')
                        next_parts = next_line.split(' ', 2)
                        if (len(next_parts) >= 2 and next_parts[0] == '2' and 
                            next_parts[1] == 'VERS'):
                            header['character_set_version'] = next_parts[2] if len(next_parts) > 2 else ''
                            i += 1
                
                elif tag == 'NOTE':
                    # Inline note in header
                    if not value.startswith('@'):
                        header['note_inline'] = value
                        note_cont, skip = self.read_continuation(lines, i, 1)
                        if note_cont:
                            header['note_inline'] += note_cont
                        i += skip
            
            i += 1
        
        # Store the parsed header
        self.header = header
        
        # Print summary of what was captured
        print(f"\nHeader Information:")
        if header['gedcom_version']:
            print(f"  GEDCOM Version: {header['gedcom_version']}")
        if header['source_system']:
            print(f"  Source System: {header['source_system']}")
            if header['source_version']:
                print(f"  Source Version: {header['source_version']}")
        if header['character_set']:
            print(f"  Character Set: {header['character_set']}")
        if header['date']:
            date_time = header['date']
            if header['time']:
                date_time += f" {header['time']}"
            print(f"  File Created: {date_time}")
        
        return i
    
    def parse_individual(self, lines, index, indi_id):
        """Parse individual record - Event-Centric Design"""
        entity = {
            'individual_id': indi_id,
            'sex': None,
            'primary_name_event_id': None,
            'primary_birth_event_id': None,
            'primary_death_event_id': None,
            'given_name': None,
            'surname': None,
            'suffix': None,
            'full_name': None,
            'sort_name': None,
            'birth_date': None,
            'birth_place': None,
            'birth_year': None,
            'death_date': None,
            'death_place': None,
            'death_year': None,
            'note_inline': None
        }
        
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            if level == 0:
                break
            
            if level != 1:
                i += 1
                continue
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            if tag == 'SEX':
                entity['sex'] = value
            
            elif tag == 'NAME':
                # Create NAME event (SINGLE SOURCE OF TRUTH)
                event_id = f"@E{self.event_counter}@"
                self.event_counter += 1
                
                name_data = self.parse_name(value)
                event_data = {
                    'event_id': event_id,
                    'event_type': 'NAME',
                    'event_type_indicator': 1,
                    'given_name': name_data['given_name'],
                    'surname': name_data['surname'],
                    'suffix': name_data['suffix'],
                    'full_name': name_data['full_name'],
                    'sort_name': name_data['sort_name']
                }
                
                self.events[event_id] = event_data
                self.indi_event_xref.append((indi_id, event_id))
                
                # First NAME = primary name
                self.name_count[indi_id] += 1
                if self.name_count[indi_id] == 1:
                    entity['primary_name_event_id'] = event_id
                    # Cache fields for Phase 2 performance
                    entity['given_name'] = name_data['given_name']
                    entity['surname'] = name_data['surname']
                    entity['suffix'] = name_data['suffix']
                    entity['full_name'] = name_data['full_name']
                    entity['sort_name'] = name_data['sort_name']
                
                # Parse sub-tags (OBJE, SOUR, NOTE under NAME)
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    
                    sub_level = sub_parts[0]
                    if not sub_level.isdigit():
                        j += 1
                        continue
                    
                    sub_level = int(sub_level)
                    
                    # Stop when we hit level 1 or 0
                    if sub_level <= 1:
                        break
                    
                    # Skip level 3+ (sub-sub-tags like OBJE under SOUR)
                    if sub_level > 2:
                        j += 1
                        continue
                    
                    # Process level 2 tags
                    sub_tag = sub_parts[1]
                    sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                    
                    if sub_tag == 'OBJE' and sub_value.startswith('@'):
                        self.event_media_xref.append((event_id, sub_value))
                    elif sub_tag == 'SOUR' and sub_value.startswith('@'):
                        source_id = sub_value
                        # Look for PAGE and OBJE at level 3
                        page_text = ''
                        
                        note_inline = ''
                        k = j + 1
                        while k < len(lines):
                            lvl3_line = lines[k].rstrip('\n\r')
                            lvl3_parts = lvl3_line.split(' ', 2)
                            if len(lvl3_parts) < 2:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) > 3:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) < 3:
                                break
                            if lvl3_parts[1] == 'PAGE':
                                page_text = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                page_cont, skip = self.read_continuation(lines, k, 3)
                                page_text += page_cont
                                k += skip  # Skip past CONC/CONT lines
                            elif lvl3_parts[1] == 'OBJE' and len(lvl3_parts) > 2 and lvl3_parts[2].startswith('@'):
                                self.citation_media_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                            elif lvl3_parts[1] == 'NOTE' and len(lvl3_parts) > 2:
                                if lvl3_parts[2].startswith('@'):
                                    self.citation_note_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                else:
                                    # Inline note text
                                    note_inline = lvl3_parts[2]
                                    note_cont, skip = self.read_continuation(lines, k, 3)
                                    note_inline += note_cont
                                    k += skip
                            elif lvl3_parts[1].startswith('_'):
                                # Capture custom tags like _LINK
                                lvl3_value = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                link_cont, skip = self.read_continuation(lines, k, 3)
                                if link_cont:
                                    lvl3_value = lvl3_value + link_cont
                                
                                parent_id_combined = f"{event_id}|{source_id}"
                                self.custom_tags.append({
                                    'parent_type': 'CITATION',
                                    'parent_id': parent_id_combined,
                                    'tag_name': lvl3_parts[1],
                                    'tag_value': lvl3_value,
                                    'tag_level': 3,
                                    'sequence_num': self._next_custom_seq('CITATION', parent_id_combined)
                                })
                                k += skip
                            k += 1
                        self.event_source_xref.append((event_id, source_id, page_text, note_inline))
                    elif sub_tag == 'NOTE' and sub_value.startswith('@'):
                        self.event_note_xref.append((event_id, sub_value))
                    
                    j += 1
                i = j - 1
            
            elif tag == 'BIRT':
                # Create BIRT event (SINGLE SOURCE OF TRUTH)
                event_id = f"@E{self.event_counter}@"
                self.event_counter += 1
                
                event_data = {
                    'event_id': event_id,
                    'event_type': 'BIRT',
                    'event_type_indicator': 1,
                    'event_date': None,
                    'event_place': None,
                    'place_latitude': None,
                    'place_longitude': None
                }
                
                # Parse BIRT sub-tags
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    
                    sub_level = sub_parts[0]
                    if not sub_level.isdigit():
                        j += 1
                        continue
                    
                    sub_level = int(sub_level)
                    
                    # Stop when we hit level 1 or 0
                    if sub_level <= 1:
                        break
                    
                    # Skip level 3+ (sub-sub-tags)
                    if sub_level > 2:
                        j += 1
                        continue
                    
                    # Process level 2 tags
                    sub_tag = sub_parts[1]
                    sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                    
                    if sub_tag == 'DATE':
                        event_data['event_date'] = sub_value
                    elif sub_tag == 'PLAC':
                        event_data['event_place'] = sub_value
                        lat, lon = self.parse_coordinates(lines, j, 2)
                        event_data['place_latitude'] = lat
                        event_data['place_longitude'] = lon
                    elif sub_tag == 'OBJE' and sub_value.startswith('@'):
                        self.event_media_xref.append((event_id, sub_value))
                    elif sub_tag == 'SOUR' and sub_value.startswith('@'):
                        source_id = sub_value
                        page_text = ''
                        
                        note_inline = ''
                        # Look for PAGE, OBJE, and NOTE at level 3
                        k = j + 1
                        while k < len(lines):
                            lvl3_line = lines[k].rstrip('\n\r')
                            lvl3_parts = lvl3_line.split(' ', 2)
                            if len(lvl3_parts) < 2:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) > 3:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) < 3:
                                break
                            if lvl3_parts[1] == 'PAGE':
                                page_text = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                page_cont, skip = self.read_continuation(lines, k, 3)
                                page_text += page_cont
                                k += skip
                            elif lvl3_parts[1] == 'OBJE' and len(lvl3_parts) > 2 and lvl3_parts[2].startswith('@'):
                                self.citation_media_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                            elif lvl3_parts[1] == 'NOTE' and len(lvl3_parts) > 2:
                                if lvl3_parts[2].startswith('@'):
                                    self.citation_note_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                else:
                                    # Inline note text
                                    note_inline = lvl3_parts[2]
                                    note_cont, skip = self.read_continuation(lines, k, 3)
                                    note_inline += note_cont
                                    k += skip
                            elif lvl3_parts[1].startswith('_'):
                                # Capture custom tags like _LINK
                                lvl3_value = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                link_cont, skip = self.read_continuation(lines, k, 3)
                                if link_cont:
                                    lvl3_value = lvl3_value + link_cont
                                
                                parent_id_combined = f"{event_id}|{source_id}"
                                self.custom_tags.append({
                                    'parent_type': 'CITATION',
                                    'parent_id': parent_id_combined,
                                    'tag_name': lvl3_parts[1],
                                    'tag_value': lvl3_value,
                                    'tag_level': 3,
                                    'sequence_num': self._next_custom_seq('CITATION', parent_id_combined)
                                })
                                k += skip
                            k += 1
                        self.event_source_xref.append((event_id, source_id, page_text, note_inline))
                    elif sub_tag == 'NOTE' and sub_value.startswith('@'):
                        self.event_note_xref.append((event_id, sub_value))
                    
                    j += 1
                
                self.events[event_id] = event_data
                self.indi_event_xref.append((indi_id, event_id))
                
                # First BIRT = primary birth
                self.birth_count[indi_id] += 1
                if self.birth_count[indi_id] == 1:
                    entity['primary_birth_event_id'] = event_id
                    # Cache fields for Phase 2 performance
                    entity['birth_date'] = event_data['event_date']
                    entity['birth_place'] = event_data['event_place']
                    entity['birth_year'] = self.extract_year(event_data['event_date'])
                
                i = j - 1
            
            elif tag == 'DEAT':
                # Create DEAT event (SINGLE SOURCE OF TRUTH)
                event_id = f"@E{self.event_counter}@"
                self.event_counter += 1
                
                event_data = {
                    'event_id': event_id,
                    'event_type': 'DEAT',
                    'event_type_indicator': 1,
                    'event_date': None,
                    'event_place': None,
                    'place_latitude': None,
                    'place_longitude': None
                }
                
                # Parse DEAT sub-tags
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    
                    sub_level = sub_parts[0]
                    if not sub_level.isdigit():
                        j += 1
                        continue
                    
                    sub_level = int(sub_level)
                    
                    # Stop when we hit level 1 or 0
                    if sub_level <= 1:
                        break
                    
                    # Skip level 3+ (sub-sub-tags)
                    if sub_level > 2:
                        j += 1
                        continue
                    
                    # Process level 2 tags
                    sub_tag = sub_parts[1]
                    sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                    
                    if sub_tag == 'DATE':
                        event_data['event_date'] = sub_value
                    elif sub_tag == 'PLAC':
                        event_data['event_place'] = sub_value
                        lat, lon = self.parse_coordinates(lines, j, 2)
                        event_data['place_latitude'] = lat
                        event_data['place_longitude'] = lon
                    elif sub_tag == 'OBJE' and sub_value.startswith('@'):
                        self.event_media_xref.append((event_id, sub_value))
                    elif sub_tag == 'SOUR' and sub_value.startswith('@'):
                        source_id = sub_value
                        page_text = ''
                        
                        note_inline = ''
                        # Look for PAGE, OBJE, and NOTE at level 3
                        k = j + 1
                        while k < len(lines):
                            lvl3_line = lines[k].rstrip('\n\r')
                            lvl3_parts = lvl3_line.split(' ', 2)
                            if len(lvl3_parts) < 2:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) > 3:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) < 3:
                                break
                            if lvl3_parts[1] == 'PAGE':
                                page_text = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                page_cont, skip = self.read_continuation(lines, k, 3)
                                page_text += page_cont
                                k += skip
                            elif lvl3_parts[1] == 'OBJE' and len(lvl3_parts) > 2 and lvl3_parts[2].startswith('@'):
                                self.citation_media_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                            elif lvl3_parts[1] == 'NOTE' and len(lvl3_parts) > 2:
                                if lvl3_parts[2].startswith('@'):
                                    self.citation_note_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                else:
                                    # Inline note text
                                    note_inline = lvl3_parts[2]
                                    note_cont, skip = self.read_continuation(lines, k, 3)
                                    note_inline += note_cont
                                    k += skip
                            elif lvl3_parts[1].startswith('_'):
                                # Capture custom tags like _LINK
                                lvl3_value = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                link_cont, skip = self.read_continuation(lines, k, 3)
                                if link_cont:
                                    lvl3_value = lvl3_value + link_cont
                                
                                parent_id_combined = f"{event_id}|{source_id}"
                                self.custom_tags.append({
                                    'parent_type': 'CITATION',
                                    'parent_id': parent_id_combined,
                                    'tag_name': lvl3_parts[1],
                                    'tag_value': lvl3_value,
                                    'tag_level': 3,
                                    'sequence_num': self._next_custom_seq('CITATION', parent_id_combined)
                                })
                                k += skip
                            k += 1
                        self.event_source_xref.append((event_id, source_id, page_text, note_inline))
                    elif sub_tag == 'NOTE' and sub_value.startswith('@'):
                        self.event_note_xref.append((event_id, sub_value))
                    
                    j += 1
                
                self.events[event_id] = event_data
                self.indi_event_xref.append((indi_id, event_id))
                
                # First DEAT = primary death
                self.death_count[indi_id] += 1
                if self.death_count[indi_id] == 1:
                    entity['primary_death_event_id'] = event_id
                    # Cache fields for Phase 2 performance
                    entity['death_date'] = event_data['event_date']
                    entity['death_place'] = event_data['event_place']
                    entity['death_year'] = self.extract_year(event_data['event_date'])
                
                i = j - 1
            
            elif tag in ['BAPM', 'BURI','CHR', 'ADOP', 'BAPL', 'CONF', 'FCOM', 'ORDN', 'NATU', 
                         'EMIG', 'IMMI', 'CENS', 'PROB', 'WILL', 'GRAD', 'RETI', 'EVEN',
                         'CAST', 'DSCR', 'EDUC', 'IDNO', 'NATI', 'NCHI', 'NMR', 'OCCU', 
                         'PROP', 'RELI', 'RESI', 'SSN', 'TITL', 'FACT', '_MILT']:
                # All other events
                event_id = f"@E{self.event_counter}@"
                self.event_counter += 1
                
                event_data = {
                    'event_id': event_id,
                    'event_type': tag,
                    'event_type_indicator': 1,
                    'event_value': value,
                    'event_date': None,
                    'event_place': None,
                    'place_latitude': None,
                    'place_longitude': None
                }
                
                # Parse event sub-tags
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    
                    sub_level = sub_parts[0]
                    if not sub_level.isdigit():
                        j += 1
                        continue
                    
                    sub_level = int(sub_level)
                    
                    # Stop when we hit level 1 or 0
                    if sub_level <= 1:
                        break
                    
                    # Skip level 3+ (sub-sub-tags)
                    if sub_level > 2:
                        j += 1
                        continue
                    
                    # Process level 2 tags
                    sub_tag = sub_parts[1]
                    sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                    
                    if sub_tag == 'TYPE':
                        event_data['event_type'] = sub_value
                    elif sub_tag == 'DATE':
                        event_data['event_date'] = sub_value
                    elif sub_tag == 'PLAC':
                        event_data['event_place'] = sub_value
                        lat, lon = self.parse_coordinates(lines, j, 2)
                        event_data['place_latitude'] = lat
                        event_data['place_longitude'] = lon
                    elif sub_tag == 'OBJE' and sub_value.startswith('@'):
                        self.event_media_xref.append((event_id, sub_value))
                    elif sub_tag == 'SOUR' and sub_value.startswith('@'):
                        source_id = sub_value
                        page_text = ''
                        
                        note_inline = ''
                        # Look for PAGE, OBJE, and NOTE at level 3
                        k = j + 1
                        while k < len(lines):
                            lvl3_line = lines[k].rstrip('\n\r')
                            lvl3_parts = lvl3_line.split(' ', 2)
                            if len(lvl3_parts) < 2:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) > 3:
                                k += 1
                                continue
                            if int(lvl3_parts[0]) < 3:
                                break
                            if lvl3_parts[1] == 'PAGE':
                                page_text = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                page_cont, skip = self.read_continuation(lines, k, 3)
                                page_text += page_cont
                                k += skip
                            elif lvl3_parts[1] == 'OBJE' and len(lvl3_parts) > 2 and lvl3_parts[2].startswith('@'):
                                self.citation_media_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                            elif lvl3_parts[1] == 'NOTE' and len(lvl3_parts) > 2:
                                if lvl3_parts[2].startswith('@'):
                                    self.citation_note_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                else:
                                    # Inline note text
                                    note_inline = lvl3_parts[2]
                                    note_cont, skip = self.read_continuation(lines, k, 3)
                                    note_inline += note_cont
                                    k += skip
                            elif lvl3_parts[1].startswith('_'):
                                # Capture custom tags like _LINK
                                lvl3_value = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                link_cont, skip = self.read_continuation(lines, k, 3)
                                if link_cont:
                                    lvl3_value = lvl3_value + link_cont
                                
                                parent_id_combined = f"{event_id}|{source_id}"
                                self.custom_tags.append({
                                    'parent_type': 'CITATION',
                                    'parent_id': parent_id_combined,
                                    'tag_name': lvl3_parts[1],
                                    'tag_value': lvl3_value,
                                    'tag_level': 3,
                                    'sequence_num': self._next_custom_seq('CITATION', parent_id_combined)
                                })
                                k += skip
                            k += 1
                        self.event_source_xref.append((event_id, source_id, page_text, note_inline))
                    elif sub_tag == 'NOTE' and sub_value.startswith('@'):
                        self.event_note_xref.append((event_id, sub_value))
                    
                    j += 1
                
                self.events[event_id] = event_data
                self.indi_event_xref.append((indi_id, event_id))
                i = j - 1
            
            elif tag == 'OBJE':
                if value.startswith('@'):
                    # Referenced OBJE (e.g., 1 OBJE @M123@)
                    if (indi_id, value, 0) not in self.indi_media_xref:
                        self.indi_media_xref.append((indi_id, value, 0))
                    else:
                        self.duplicate_stats['indi_media'] += 1
                else:
                    # Inline OBJE (e.g., 1 OBJE\n2 FILE...)
                    i = self.parse_inline_media(lines, i, 'INDI', indi_id, level)
                    continue  # parse_inline_media already advanced i, don't increment again
            
            elif tag == 'NOTE' and value.startswith('@'):
                self.indi_note_xref.append((indi_id, value))
            
            elif tag == 'SOUR' and value.startswith('@'):
                # Individual-level source citation (not attached to event)
                source_id = value
                page_text = ''
                
                note_inline = ''
                # Look for PAGE, OBJE, NOTE at level 2
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    if not sub_parts[0].isdigit():
                        j += 1
                        continue
                    if int(sub_parts[0]) <= 1:
                        break
                    if int(sub_parts[0]) == 2:
                        if sub_parts[1] == 'PAGE':
                            page_text = sub_parts[2] if len(sub_parts) > 2 else ''
                            page_cont, skip = self.read_continuation(lines, j, 2)
                            page_text += page_cont
                            j += skip
                        elif sub_parts[1] == 'OBJE' and len(sub_parts) > 2 and sub_parts[2].startswith('@'):
                            self.indi_citation_media_xref.append((indi_id, source_id, sub_parts[2]))
                        elif sub_parts[1] == 'NOTE' and len(sub_parts) > 2:
                            if sub_parts[2].startswith('@'):
                                self.indi_citation_note_xref.append((indi_id, source_id, sub_parts[2]))
                            else:
                                # Inline note text
                                note_inline = sub_parts[2]
                                note_cont, skip = self.read_continuation(lines, j, 2)
                                note_inline += note_cont
                                j += skip
                    j += 1
                self.indi_source_xref.append((indi_id, source_id, page_text, note_inline))
            
            elif tag == 'NOTE' and not value.startswith('@'):
                # Inline note
                note_text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                note_text += cont_text
                entity['note_inline'] = note_text
                i += skip
            
            elif tag == 'SOUR' and value.startswith('@'):
                self.indi_source_xref.append((indi_id, value))
            
            elif tag == 'FAMC' and value.startswith('@'):
                self.child_family_xref.append((value, indi_id, 0))
            
            elif tag == 'FAMS' and value.startswith('@'):
                self.spouse_family_xref.append((indi_id, value))
            
            i += 1
        
        # Capture any custom tags attached to this individual
        self.capture_custom_tags(lines, index, 0, 'INDI', indi_id)
        
        self.individuals[indi_id] = entity
        return i
    
    def parse_family(self, lines, index, fam_id):
        """Parse family record"""
        entity = {
            'family_id': fam_id,
            'husband_id': None,
            'wife_id': None,
            'marriage_date': None,
            'marriage_place': None,
            'note_inline': None
        }
        
        i = index + 1
        child_order = 0
        
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            if level == 0:
                break
            
            if level != 1:
                i += 1
                continue
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            if tag == 'HUSB' and value.startswith('@'):
                entity['husband_id'] = value
            
            elif tag == 'WIFE' and value.startswith('@'):
                entity['wife_id'] = value
            
            elif tag == 'CHIL' and value.startswith('@'):
                child_order += 1
                child_id = value
                self.child_family_xref.append((fam_id, child_id, child_order))
                
                # Capture _FREL and _MREL tags under this CHIL
                parent_id_combined = f"{fam_id}|{child_id}"
                self.capture_custom_tags(lines, i, 1, 'CHIL', parent_id_combined)
            
            elif tag == 'MARR':
                # Create MARR event (proper event-centric design!)
                event_id = f"@E{self.event_counter}@"
                self.event_counter += 1
                
                event_data = {
                    'event_id': event_id,
                    'event_type': 'MARR',
                    'event_type_indicator': 2,  # Family event
                    'event_date': None,
                    'event_place': None,
                    'place_latitude': None,
                    'place_longitude': None
                }
                
                # Parse MARR sub-tags
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    
                    sub_level = sub_parts[0]
                    if not sub_level.isdigit():
                        j += 1
                        continue
                    
                    sub_level = int(sub_level)
                    
                    if sub_level <= 1:
                        break
                    
                    if sub_level == 2:
                        sub_tag = sub_parts[1]
                        sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                        
                        if sub_tag == 'DATE':
                            event_data['event_date'] = sub_value
                            entity['marriage_date'] = sub_value  # Cache in families table
                        elif sub_tag == 'PLAC':
                            event_data['event_place'] = sub_value
                            entity['marriage_place'] = sub_value  # Cache in families table
                            # Parse coordinates!
                            lat, lon = self.parse_coordinates(lines, j, 2)
                            event_data['place_latitude'] = lat
                            event_data['place_longitude'] = lon
                        elif sub_tag == 'OBJE' and sub_value.startswith('@'):
                            self.event_media_xref.append((event_id, sub_value))
                        elif sub_tag == 'SOUR' and sub_value.startswith('@'):
                            source_id = sub_value
                            page_text = ''
                            
                            note_inline = ''
                            # Look for PAGE and OBJE at level 3
                            k = j + 1
                            while k < len(lines):
                                lvl3_line = lines[k].rstrip('\n\r')
                                lvl3_parts = lvl3_line.split(' ', 2)
                                if len(lvl3_parts) < 2:
                                    k += 1
                                    continue
                                if int(lvl3_parts[0]) > 3:
                                    k += 1
                                    continue
                                if int(lvl3_parts[0]) < 3:
                                    break
                                if lvl3_parts[1] == 'PAGE':
                                    new_page = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                    page_cont, skip = self.read_continuation(lines, k, 3)
                                    new_page += page_cont
                                    # Concatenate multiple PAGE lines (non-standard but occurs in data)
                                    if page_text:
                                        page_text += ' ' + new_page
                                    else:
                                        page_text = new_page
                                    k += skip
                                elif lvl3_parts[1] == 'OBJE' and len(lvl3_parts) > 2 and lvl3_parts[2].startswith('@'):
                                    self.citation_media_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                elif lvl3_parts[1] == 'NOTE' and len(lvl3_parts) > 2:
                                    if lvl3_parts[2].startswith('@'):
                                        self.citation_note_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                    else:
                                        # Inline note text
                                        note_inline = lvl3_parts[2]
                                        note_cont, skip = self.read_continuation(lines, k, 3)
                                        note_inline += note_cont
                                        k += skip
                                elif lvl3_parts[1].startswith('_'):
                                    # Capture custom tags like _LINK
                                    lvl3_value = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                    link_cont, skip = self.read_continuation(lines, k, 3)
                                    if link_cont:
                                        lvl3_value = lvl3_value + link_cont
                                    
                                    parent_id_combined = f"{event_id}|{source_id}"
                                    self.custom_tags.append({
                                        'parent_type': 'CITATION',
                                        'parent_id': parent_id_combined,
                                        'tag_name': lvl3_parts[1],
                                        'tag_value': lvl3_value,
                                        'tag_level': 3,
                                        'sequence_num': self._next_custom_seq('CITATION', parent_id_combined)
                                    })
                                    k += skip
                                k += 1
                            self.event_source_xref.append((event_id, source_id, page_text, note_inline))
                        elif sub_tag == 'NOTE' and sub_value.startswith('@'):
                            self.event_note_xref.append((event_id, sub_value))
                    
                    j += 1
                
                # Store event and link to family
                self.events[event_id] = event_data
                self.fam_event_xref.append((fam_id, event_id))
                
                i = j - 1
            
            elif tag == 'DIV':
                # Create DIV event (divorce)
                event_id = f"@E{self.event_counter}@"
                self.event_counter += 1
                
                event_data = {
                    'event_id': event_id,
                    'event_type': 'DIV',
                    'event_type_indicator': 2,  # Family event
                    'event_date': None,
                    'event_place': None,
                    'place_latitude': None,
                    'place_longitude': None
                }
                
                # Parse DIV sub-tags
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    
                    sub_level = sub_parts[0]
                    if not sub_level.isdigit():
                        j += 1
                        continue
                    
                    sub_level = int(sub_level)
                    
                    if sub_level <= 1:
                        break
                    
                    if sub_level == 2:
                        sub_tag = sub_parts[1]
                        sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                        
                        if sub_tag == 'DATE':
                            event_data['event_date'] = sub_value
                        elif sub_tag == 'PLAC':
                            event_data['event_place'] = sub_value
                            # Parse coordinates!
                            lat, lon = self.parse_coordinates(lines, j, 2)
                            event_data['place_latitude'] = lat
                            event_data['place_longitude'] = lon
                        elif sub_tag == 'OBJE' and sub_value.startswith('@'):
                            self.event_media_xref.append((event_id, sub_value))
                        elif sub_tag == 'SOUR' and sub_value.startswith('@'):
                            source_id = sub_value
                            page_text = ''
                            
                            note_inline = ''
                            # Look for PAGE and OBJE at level 3
                            k = j + 1
                            while k < len(lines):
                                lvl3_line = lines[k].rstrip('\n\r')
                                lvl3_parts = lvl3_line.split(' ', 2)
                                if len(lvl3_parts) < 2:
                                    k += 1
                                    continue
                                if int(lvl3_parts[0]) > 3:
                                    k += 1
                                    continue
                                if int(lvl3_parts[0]) < 3:
                                    break
                                if lvl3_parts[1] == 'PAGE':
                                    new_page = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                    page_cont, skip = self.read_continuation(lines, k, 3)
                                    new_page += page_cont
                                    # Concatenate multiple PAGE lines (non-standard but occurs in data)
                                    if page_text:
                                        page_text += ' ' + new_page
                                    else:
                                        page_text = new_page
                                    k += skip
                                elif lvl3_parts[1] == 'OBJE' and len(lvl3_parts) > 2 and lvl3_parts[2].startswith('@'):
                                    self.citation_media_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                elif lvl3_parts[1] == 'NOTE' and len(lvl3_parts) > 2:
                                    if lvl3_parts[2].startswith('@'):
                                        self.citation_note_xref.append((event_id, source_id, page_text, lvl3_parts[2]))
                                    else:
                                        # Inline note text
                                        note_inline = lvl3_parts[2]
                                        note_cont, skip = self.read_continuation(lines, k, 3)
                                        note_inline += note_cont
                                        k += skip
                                elif lvl3_parts[1].startswith('_'):
                                    # Capture custom tags like _LINK
                                    lvl3_value = lvl3_parts[2] if len(lvl3_parts) > 2 else ''
                                    link_cont, skip = self.read_continuation(lines, k, 3)
                                    if link_cont:
                                        lvl3_value = lvl3_value + link_cont
                                    
                                    parent_id_combined = f"{event_id}|{source_id}"
                                    self.custom_tags.append({
                                        'parent_type': 'CITATION',
                                        'parent_id': parent_id_combined,
                                        'tag_name': lvl3_parts[1],
                                        'tag_value': lvl3_value,
                                        'tag_level': 3,
                                        'sequence_num': self._next_custom_seq('CITATION', parent_id_combined)
                                    })
                                    k += skip
                                k += 1
                            self.event_source_xref.append((event_id, source_id, page_text, note_inline))
                        elif sub_tag == 'NOTE' and sub_value.startswith('@'):
                            self.event_note_xref.append((event_id, sub_value))
                    
                    j += 1
                
                # Store event and link to family
                self.events[event_id] = event_data
                self.fam_event_xref.append((fam_id, event_id))
                
                i = j - 1
            
            elif tag == 'NOTE' and not value.startswith('@'):
                note_text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                note_text += cont_text
                entity['note_inline'] = note_text
                i += skip
            
            elif tag == 'NOTE' and value.startswith('@'):
                # Family-level NOTE reference
                self.fam_note_xref.append((fam_id, value))
            
            elif tag == 'OBJE':
                if value.startswith('@'):
                    # Referenced OBJE (e.g., 1 OBJE @M123@)
                    if (fam_id, value, 0) not in self.fam_media_xref:
                        self.fam_media_xref.append((fam_id, value, 0))
                    else:
                        self.duplicate_stats['fam_media'] += 1
                else:
                    # Inline OBJE (e.g., 1 OBJE\n2 FILE...)
                    i = self.parse_inline_media(lines, i, 'FAM', fam_id, level)
                    continue  # parse_inline_media already advanced i, don't increment again
            
            elif tag == 'SOUR' and value.startswith('@'):
                # Family-level source citation (not under MARR/DIV)
                source_id = value
                page_text = ''
                
                note_inline = ''
                # Look for PAGE, OBJE, NOTE at level 2
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].rstrip('\n\r')
                    sub_parts = sub_line.split(' ', 2)
                    if len(sub_parts) < 2:
                        j += 1
                        continue
                    if not sub_parts[0].isdigit():
                        j += 1
                        continue
                    if int(sub_parts[0]) <= 1:
                        break
                    if int(sub_parts[0]) == 2:
                        if sub_parts[1] == 'PAGE':
                            page_text = sub_parts[2] if len(sub_parts) > 2 else ''
                            page_cont, skip = self.read_continuation(lines, j, 2)
                            page_text += page_cont
                            j += skip
                        elif sub_parts[1] == 'OBJE' and len(sub_parts) > 2 and sub_parts[2].startswith('@'):
                            self.fam_citation_media_xref.append((fam_id, source_id, sub_parts[2]))
                        elif sub_parts[1] == 'NOTE' and len(sub_parts) > 2:
                            if sub_parts[2].startswith('@'):
                                self.fam_citation_note_xref.append((fam_id, source_id, sub_parts[2]))
                            else:
                                # Inline note text
                                note_inline = sub_parts[2]
                                note_cont, skip = self.read_continuation(lines, j, 2)
                                note_inline += note_cont
                                j += skip
                    j += 1
                self.fam_source_xref.append((fam_id, source_id, page_text, note_inline))
            
            i += 1
        
        # Capture any custom tags attached to this family
        self.capture_custom_tags(lines, index, 0, 'FAM', fam_id)
        
        self.families[fam_id] = entity
        return i
    
    def parse_note(self, lines, index, note_id):
        """Parse note record"""
        line = lines[index].rstrip('\n\r')
        parts = line.split(' ', 3)
        note_text = parts[3] if len(parts) > 3 else ''
        
        # Read continuation lines
        cont_text, skip = self.read_continuation(lines, index, 0)
        note_text += cont_text
        
        # Parse level 1 tags under NOTE (like OBJE)
        i = index + skip + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 3)
            if len(parts) < 2:
                i += 1
                continue
            if not parts[0].isdigit():
                i += 1
                continue
            level = int(parts[0])
            if level == 0:
                break
            if level == 1:
                tag = parts[1]
                value = parts[2] if len(parts) > 2 else ''
                if tag == 'OBJE' and value.startswith('@'):
                    self.note_media_xref.append((note_id, value))
            i += 1
        
        # Capture any custom tags attached to this note
        self.capture_custom_tags(lines, index, 0, 'NOTE', note_id)
        
        self.notes[note_id] = {'note_id': note_id, 'note_text': note_text}
        return i
    
    def parse_source(self, lines, index, source_id):
        """Parse source record"""
        entity = {
            'source_id': source_id,
            'title': None,
            'author': None,
            'publication_info': None,
            'abbreviation': None,
            'text': None,
            'note_inline': None
        }
        
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            if level == 0:
                break
            
            if level != 1:
                i += 1
                continue
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            if tag == 'TITL':
                text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                entity['title'] = text + cont_text
                i += skip
            elif tag == 'AUTH':
                text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                entity['author'] = text + cont_text
                i += skip
            elif tag == 'PUBL':
                text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                entity['publication_info'] = text + cont_text
                i += skip
            elif tag == 'ABBR':
                entity['abbreviation'] = value
            elif tag == 'TEXT':
                text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                entity['text'] = text + cont_text
                i += skip
            elif tag == 'REPO' and value.startswith('@'):
                # Repository reference with optional CALN
                call_number = None
                if i + 1 < len(lines):
                    next_line = lines[i + 1].rstrip('\n\r')
                    next_parts = next_line.split(' ', 2)
                    if len(next_parts) >= 2 and next_parts[0] == '2' and next_parts[1] == 'CALN':
                        call_number = next_parts[2] if len(next_parts) > 2 else ''
                self.source_repo_xref.append((source_id, value, call_number))
            elif tag == 'OBJE' and value.startswith('@'):
                # Source-level media reference
                self.source_media_xref.append((source_id, value))
            elif tag == 'NOTE' and value.startswith('@'):
                # Source-level note reference
                self.source_note_xref.append((source_id, value))
            elif tag == 'NOTE' and not value.startswith('@'):
                # Inline NOTE text on source record
                text = value
                cont_text, skip = self.read_continuation(lines, i, 1)
                entity['note_inline'] = text + cont_text
                i += skip
            
            i += 1
        
        # Capture any custom tags attached to this source
        self.capture_custom_tags(lines, index, 0, 'SOURCE', source_id)
        
        self.sources[source_id] = entity
        return i
    
    def parse_media(self, lines, index, media_id):
        """Parse media record - FIXED to handle level 2 FORM/TITL under FILE"""
        entity = {
            'media_id': media_id,
            'file_path': None,
            'format': None,
            'title': None,
            'note_inline': None
        }
        
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            if level == 0:
                break
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            if level == 1:
                if tag == 'FILE':
                    entity['file_path'] = value
                    # Look ahead for level 2 FORM/TITL under FILE
                    j = i + 1
                    while j < len(lines):
                        sub_line = lines[j].rstrip('\n\r')
                        sub_parts = sub_line.split(' ', 2)
                        if len(sub_parts) < 2:
                            j += 1
                            continue
                        
                        sub_level = sub_parts[0]
                        if not sub_level.isdigit():
                            j += 1
                            continue
                        
                        sub_level = int(sub_level)
                        if sub_level <= 1:
                            break
                        
                        if sub_level == 2:
                            sub_tag = sub_parts[1]
                            sub_value = sub_parts[2] if len(sub_parts) > 2 else ''
                            
                            if sub_tag == 'FORM':
                                entity['format'] = sub_value
                            elif sub_tag == 'TITL':
                                cont_text, lines_consumed = self.read_continuation(lines, j, sub_level)
                                entity['title'] = sub_value + cont_text
                                j += lines_consumed
                            elif sub_tag.startswith('_'):
                                # Capture custom tags like _DATE and _TEXT
                                cont_text, lines_consumed = self.read_continuation(lines, j, sub_level)
                                if cont_text:
                                    sub_value = sub_value + cont_text
                                
                                self.custom_tags.append({
                                    'parent_type': 'MEDIA',
                                    'parent_id': media_id,
                                    'tag_name': sub_tag,
                                    'tag_value': sub_value,
                                    'tag_level': sub_level,
                                    'sequence_num': self._next_custom_seq('MEDIA', media_id)
                                })
                                j += lines_consumed
                                entity['title'] = sub_value
                        
                        j += 1
                    i = j - 1
                elif tag == 'FORM':
                    entity['format'] = value
                elif tag == 'TITL':
                    entity['title'] = value
                elif tag == 'NOTE':
                    # Read NOTE with continuation lines (CONC/CONT)
                    note_text, lines_consumed = self.read_continuation(lines, i, level)
                    entity['note_inline'] = value + note_text
                    i += lines_consumed
            
            i += 1
        
        self.media[media_id] = entity
        return i
    
    def parse_inline_media(self, lines, index, parent_type, parent_id, start_level):
        """Parse inline OBJE structure (no @ID@) and return media data
        
        Handles structures like:
        1 OBJE
        2 FILE path/to/file.jpg
        2 FORM jpg
        2 TITL title
        2 NOTE note text
        3 CONC continuation
        """
        
        media_data = {
            'file_path': None,
            'format': None,
            'title': None,
            'note_inline': None
        }
        
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            
            # Exit when we return to start level or lower
            if level <= start_level:
                break
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            # Level 2 tags under OBJE (or start_level + 1)
            if level == start_level + 1:
                if tag == 'FILE':
                    media_data['file_path'] = value
                elif tag == 'FORM':
                    media_data['format'] = value
                elif tag == 'TITL':
                    media_data['title'] = value
                elif tag == 'NOTE':
                    # Read NOTE with continuation lines (CONC/CONT)
                    note_text, lines_consumed = self.read_continuation(lines, i, level)
                    media_data['note_inline'] = value + note_text
                    i += lines_consumed
            
            i += 1
        
        # Only create media if we have at least a file path
        if media_data['file_path']:
            # Generate unique media ID
            media_id = f"@M{len(self.media) + 1}@"
            media_data['media_id'] = media_id
            
            self.media[media_id] = media_data
            
            # Create cross-reference based on parent type
            if parent_type == 'INDI':
                if (parent_id, media_id, 0) not in self.indi_media_xref:
                    self.indi_media_xref.append((parent_id, media_id, 0))
            elif parent_type == 'FAM':
                if (parent_id, media_id, 0) not in self.fam_media_xref:
                    self.fam_media_xref.append((parent_id, media_id, 0))
            elif parent_type == 'EVENT':
                if (parent_id, media_id, 0) not in self.event_media_xref:
                    self.event_media_xref.append((parent_id, media_id, 0))
            elif parent_type == 'SOUR':
                if (parent_id, media_id, 0) not in self.source_media_xref:
                    self.source_media_xref.append((parent_id, media_id, 0))
        
        return i
    
    def parse_repository(self, lines, index, repo_id):
        """Parse repository record"""
        entity = {
            'repository_id': repo_id,
            'name': None,
            'address': None,
            'email': None,
            'phone': None,
            'note_inline': None
        }
        
        i = index + 1
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            parts = line.split(' ', 2)
            if len(parts) < 2:
                i += 1
                continue
            
            level = parts[0]
            if not level.isdigit():
                i += 1
                continue
            
            level = int(level)
            if level == 0:
                break
            
            if level != 1:
                i += 1
                continue
            
            tag = parts[1]
            value = parts[2] if len(parts) > 2 else ''
            
            if tag == 'NAME':
                entity['name'] = value
            elif tag == 'ADDR':
                entity['address'] = value
            elif tag == 'EMAIL':
                entity['email'] = value
            elif tag == 'PHON':
                entity['phone'] = value
            
            i += 1
        
        # Capture any custom tags attached to this repository
        self.capture_custom_tags(lines, index, 0, 'REPO', repo_id)
        
        self.repositories[repo_id] = entity
        return i
    
    def write_to_database(self, *, clear_mode: Optional[str] = None, chunk_size: Optional[int] = None,
                         validate_post_load: Optional[bool] = None) -> None:
        """Write all parsed data to MySQL database.

        Parameters:
          clear_mode: 'truncate' | 'delete' | 'none' (defaults to self.clear_mode)
          chunk_size: batch size for executemany() (defaults to self.chunk_size)
          validate_post_load: run basic post-load integrity checks (defaults to self.validate_post_load)
        """
        if clear_mode is not None:
            self.clear_mode = (clear_mode or "truncate").lower()
        if chunk_size is not None:
            self.chunk_size = int(chunk_size)
        if validate_post_load is not None:
            self.validate_post_load = bool(validate_post_load)

        print("\nWriting to database...")

        # Extract unique places from events BEFORE clearing tables
        self.extract_places_from_events()

        try:
            self.conn.start_transaction()
            self.clear_database(self.clear_mode)

            # Perform inserts (no commit here)
            self._write_to_database_body()

            self.conn.commit()
        except Exception as exc:
            try:
                self.conn.rollback()
            except Exception:
                pass
            print(f"\nERROR: Database load failed and was rolled back: {exc}")
            raise

        # Report duplicates that were skipped
        total_duplicates = sum(self.duplicate_stats.values())
        if total_duplicates > 0:
            print("\n  Data Quality Improvements:")
            print("    Duplicate media references removed:")
            if self.duplicate_stats.get('indi_media', 0) > 0:
                print(f"      Individual media: {self.duplicate_stats['indi_media']}")
            if self.duplicate_stats.get('fam_media', 0) > 0:
                print(f"      Family media: {self.duplicate_stats['fam_media']}")
            if self.duplicate_stats.get('event_media', 0) > 0:
                print(f"      Event media: {self.duplicate_stats['event_media']}")
            if self.duplicate_stats.get('citation_media', 0) > 0:
                print(f"      Citation media: {self.duplicate_stats['citation_media']}")
            print(f"    Total duplicates removed: {total_duplicates}")

        if self.validate_post_load:
            self.post_load_validate()

        print("\nDatabase import complete!")


    def _write_header_to_database(self) -> None:
        """Write header information to database"""
        if self.header:
            header_tuple = (
                self.header.get('gedcom_version'),
                self.header.get('gedcom_form'),
                self.header.get('source_system'),
                self.header.get('source_version'),
                self.header.get('source_name'),
                self.header.get('source_corporation'),
                self.header.get('corp_address'),
                self.header.get('corp_phone'),
                self.header.get('source_data'),
                self.header.get('destination'),
                self.header.get('date'),
                self.header.get('time'),
                self.header.get('submitter_id'),
                self.header.get('submission_id'),
                self.header.get('file_name'),
                self.header.get('copyright'),
                self.header.get('language'),
                self.header.get('character_set'),
                self.header.get('character_set_version'),
                self.header.get('note_inline')
            )
            
            self.cursor.execute('''
                INSERT INTO header (
                    gedcom_version, gedcom_form, source_system, source_version,
                    source_name, source_corporation, corp_address, corp_phone,
                    source_data, destination,
                    date, time, submitter_id, submission_id, file_name,
                    copyright, language, character_set, character_set_version,
                    note_inline
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', header_tuple)
            
            print(f"  Inserted header information")

    def _write_to_database_body(self) -> None:
        """Insert core tables and xref tables (expects an open transaction)."""
        
        # Insert header FIRST
        self._write_header_to_database()
        
        # Insert events (MUST BE FIRST - referenced by individuals)
        if self.events:
            event_tuples = []
            for event_id, event in self.events.items():
                event_tuples.append((
                    event.get('event_id'),
                    event.get('event_type'),
                    event.get('event_type_indicator'),
                    event.get('given_name'),
                    event.get('surname'),
                    event.get('suffix'),
                    event.get('full_name'),
                    event.get('sort_name'),
                    event.get('event_date'),
                    event.get('event_place'),
                    event.get('event_value'),
                    event.get('event_responsible'),
                    event.get('event_age'),
                    event.get('place_latitude'),
                    event.get('place_longitude'),
                    event.get('note_inline')
                ))
            
            self.executemany_chunked('''
                INSERT INTO events (
                    event_id, event_type, event_type_indicator,
                    given_name, surname, suffix, full_name, sort_name,
                    event_date, event_place, event_value, event_responsible, event_age,
                    place_latitude, place_longitude, note_inline
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', event_tuples)
            print(f"  Inserted {len(event_tuples)} events")
        
        # Insert individuals (references events via foreign keys)
        if self.individuals:
            indi_tuples = []
            for indi_id, indi in self.individuals.items():
                indi_tuples.append((
                    indi.get('individual_id'),
                    indi.get('sex'),
                    indi.get('primary_name_event_id'),
                    indi.get('primary_birth_event_id'),
                    indi.get('primary_death_event_id'),
                    indi.get('given_name'),
                    indi.get('surname'),
                    indi.get('suffix'),
                    indi.get('full_name'),
                    indi.get('sort_name'),
                    indi.get('birth_date'),
                    indi.get('birth_place'),
                    indi.get('birth_year'),
                    indi.get('death_date'),
                    indi.get('death_place'),
                    indi.get('death_year'),
                    indi.get('note_inline')
                ))
            
            self.executemany_chunked('''
                INSERT INTO individuals (
                    individual_id, sex,
                    primary_name_event_id, primary_birth_event_id, primary_death_event_id,
                    given_name, surname, suffix, full_name, sort_name,
                    birth_date, birth_place, birth_year,
                    death_date, death_place, death_year,
                    note_inline
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', indi_tuples)
            print(f"  Inserted {len(indi_tuples)} individuals")
        
        # Insert families
        if self.families:
            fam_tuples = []
            for f in self.families.values():
                husband_id = f.get('husband_id')
                wife_id    = f.get('wife_id')
                # Guard against FK violation: only use ID if individual was parsed
                if husband_id and husband_id not in self.individuals:
                    print(f"  WARNING: family {f.get('family_id')} husband {husband_id} not found in individuals — set to NULL")
                    husband_id = None
                if wife_id and wife_id not in self.individuals:
                    print(f"  WARNING: family {f.get('family_id')} wife {wife_id} not found in individuals — set to NULL")
                    wife_id = None
                fam_tuples.append((
                    f.get('family_id'), husband_id, wife_id,
                    f.get('marriage_date'), f.get('marriage_place'), f.get('note_inline')
                ))
            self.executemany_chunked('''
                INSERT INTO families (family_id, husband_id, wife_id, marriage_date, marriage_place, note_inline)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', fam_tuples)
            print(f"  Inserted {len(fam_tuples)} families")
        
        # Insert notes
        if self.notes:
            note_tuples = [(n.get('note_id'), n.get('note_text')) for n in self.notes.values()]
            self.executemany_chunked('INSERT INTO notes (note_id, note_text) VALUES (%s, %s)', note_tuples)
            print(f"  Inserted {len(note_tuples)} notes")
        
        # Insert sources
        if self.sources:
            source_tuples = [(s.get('source_id'), s.get('title'), s.get('author'),
                            s.get('publication_info'), s.get('abbreviation'), s.get('text'),
                            s.get('note_inline')) for s in self.sources.values()]
            self.executemany_chunked('''
                INSERT INTO sources (source_id, title, author, publication_info, abbreviation, text, note_inline)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', source_tuples)
            print(f"  Inserted {len(source_tuples)} sources")
        
        # Insert media
        if self.media:
            media_tuples = [(m.get('media_id'), m.get('file_path'), m.get('format'),
                           m.get('title'), m.get('note_inline')) for m in self.media.values()]
            self.executemany_chunked('''
                INSERT INTO media (media_id, file_path, format, title, note_inline)
                VALUES (%s, %s, %s, %s, %s)
            ''', media_tuples)
            print(f"  Inserted {len(media_tuples)} media")
        
        # Insert places
        if self.places:
            place_tuples = [
                (p.get('place_id'), p.get('place_name'),
                 p.get('place_latitude'), p.get('place_longitude'),
                 p.get('db_city'), p.get('db_county'),
                 p.get('db_state'), p.get('db_country'),
                 p.get('note_inline'))
                for p in self.places.values()
            ]
            self.executemany_chunked('''
                INSERT INTO places (place_id, place_name, place_latitude, place_longitude,
                                    db_city, db_county, db_state, db_country, note_inline)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', place_tuples)
            print(f"  Inserted {len(place_tuples)} places")
        
        # Insert repositories
        if self.repositories:
            repo_tuples = [(r.get('repository_id'), r.get('name'), r.get('address'),
                          r.get('email'), r.get('phone'),
                          r.get('note_inline')) for r in self.repositories.values()]
            self.executemany_chunked('''
                INSERT INTO repositories (repository_id, name, address, email, phone, note_inline)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', repo_tuples)
            print(f"  Inserted {len(repo_tuples)} repositories")
        
        # Insert cross-references
        xref_counts = {}
        
        if self.indi_event_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO indi_event_xref (individual_id, event_id) VALUES (%s, %s)
            ''', self.indi_event_xref)
            xref_counts['indi_event_xref'] = len(self.indi_event_xref)
        
        if self.indi_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO indi_media_xref (individual_id, media_id, indi_media_primary)
                VALUES (%s, %s, %s)
            ''', self.indi_media_xref)
            xref_counts['indi_media_xref'] = len(self.indi_media_xref)
        
        if self.indi_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO indi_note_xref (individual_id, note_id) VALUES (%s, %s)
            ''', self.indi_note_xref)
            xref_counts['indi_note_xref'] = len(self.indi_note_xref)
        
        if self.indi_source_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO indi_source_xref (individual_id, source_id, citation_page, note_inline) VALUES (%s, %s, %s, %s)
            ''', self.indi_source_xref)
            xref_counts['indi_source_xref'] = len(self.indi_source_xref)
        
        if self.fam_source_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO fam_source_xref (family_id, source_id, citation_page, note_inline) VALUES (%s, %s, %s, %s)
            ''', self.fam_source_xref)
            xref_counts['fam_source_xref'] = len(self.fam_source_xref)
        
        if self.fam_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO fam_media_xref (family_id, media_id) VALUES (%s, %s)
            ''', self.fam_media_xref)
            xref_counts['fam_media_xref'] = len(self.fam_media_xref)
        
        if self.fam_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO fam_note_xref (family_id, note_id) VALUES (%s, %s)
            ''', self.fam_note_xref)
            xref_counts['fam_note_xref'] = len(self.fam_note_xref)
        
        if self.source_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO source_media_xref (source_id, media_id) VALUES (%s, %s)
            ''', self.source_media_xref)
            xref_counts['source_media_xref'] = len(self.source_media_xref)
        
        if self.source_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO source_note_xref (source_id, note_id) VALUES (%s, %s)
            ''', self.source_note_xref)
            xref_counts['source_note_xref'] = len(self.source_note_xref)
        
        if self.note_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO note_media_xref (note_id, media_id) VALUES (%s, %s)
            ''', self.note_media_xref)
            xref_counts['note_media_xref'] = len(self.note_media_xref)
        
        if self.fam_citation_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO fam_citation_media_xref (family_id, source_id, media_id) VALUES (%s, %s, %s)
            ''', self.fam_citation_media_xref)
            xref_counts['fam_citation_media_xref'] = len(self.fam_citation_media_xref)
        
        if self.fam_citation_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO fam_citation_note_xref (family_id, source_id, note_id) VALUES (%s, %s, %s)
            ''', self.fam_citation_note_xref)
            xref_counts['fam_citation_note_xref'] = len(self.fam_citation_note_xref)
        
        if self.indi_citation_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO indi_citation_media_xref (individual_id, source_id, media_id) VALUES (%s, %s, %s)
            ''', self.indi_citation_media_xref)
            xref_counts['indi_citation_media_xref'] = len(self.indi_citation_media_xref)
        
        if self.indi_citation_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO indi_citation_note_xref (individual_id, source_id, note_id) VALUES (%s, %s, %s)
            ''', self.indi_citation_note_xref)
            xref_counts['indi_citation_note_xref'] = len(self.indi_citation_note_xref)
        
        if self.event_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO event_media_xref (event_id, media_id) VALUES (%s, %s)
            ''', self.event_media_xref)
            xref_counts['event_media_xref'] = len(self.event_media_xref)
        
        if self.event_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO event_note_xref (event_id, note_id) VALUES (%s, %s)
            ''', self.event_note_xref)
            xref_counts['event_note_xref'] = len(self.event_note_xref)
        
        if self.event_source_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO event_source_xref (event_id, source_id, page, note_inline) VALUES (%s, %s, %s, %s)
            ''', self.event_source_xref)
            xref_counts['event_source_xref'] = len(self.event_source_xref)
        
        if self.child_family_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO child_family_xref (family_id, child_id, child_order)
                VALUES (%s, %s, %s)
            ''', self.child_family_xref)
            xref_counts['child_family_xref'] = len(self.child_family_xref)
        
        if self.spouse_family_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO spouse_family_xref (individual_id, family_id) VALUES (%s, %s)
            ''', self.spouse_family_xref)
            xref_counts['spouse_family_xref'] = len(self.spouse_family_xref)
        
        if self.fam_event_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO fam_event_xref (family_id, event_id) VALUES (%s, %s)
            ''', self.fam_event_xref)
            xref_counts['fam_event_xref'] = len(self.fam_event_xref)
        
        if self.citation_media_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO citation_media_xref (event_id, source_id, citation_page, media_id) VALUES (%s, %s, %s, %s)
            ''', self.citation_media_xref)
            xref_counts['citation_media_xref'] = len(self.citation_media_xref)
        
        if self.source_repo_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO source_repo_xref (source_id, repository_id, call_number) VALUES (%s, %s, %s)
            ''', self.source_repo_xref)
            xref_counts['source_repo_xref'] = len(self.source_repo_xref)

        if self.citation_note_xref:
            self.executemany_chunked('''
                INSERT IGNORE INTO citation_note_xref (event_id, source_id, citation_page, note_id) VALUES (%s, %s, %s, %s)
            ''', self.citation_note_xref)
            xref_counts['citation_note_xref'] = len(self.citation_note_xref)
        
        if self.custom_tags:
            custom_tag_tuples = [
                (ct['parent_type'], ct['parent_id'], ct['tag_name'], 
                 ct['tag_value'], ct['tag_level'], ct['sequence_num'])
                for ct in self.custom_tags
            ]
            self.executemany_chunked('''
                INSERT INTO custom_tags (parent_type, parent_id, tag_name, tag_value, tag_level, sequence_num)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', custom_tag_tuples)
            xref_counts['custom_tags'] = len(custom_tag_tuples)
        
        print("\n  Cross-references:")
        for table, count in xref_counts.items():
            print(f"    {table}: {count}")
        
        

    def post_load_validate(self) -> None:
        """Run basic post-load validation queries (best-effort)."""
        print("\nPOST-LOAD VALIDATION")
        print("-" * 80)

        checks = [
            ("media rows", "SELECT COUNT(*) FROM media"),
            ("individual rows", "SELECT COUNT(*) FROM individuals"),
            ("family rows", "SELECT COUNT(*) FROM families"),
            ("event rows", "SELECT COUNT(*) FROM events"),
            ("indi media xref rows", "SELECT COUNT(*) FROM indi_media_xref"),
            ("fam  media xref rows", "SELECT COUNT(*) FROM fam_media_xref"),
            ("event media xref rows", "SELECT COUNT(*) FROM event_media_xref"),
        ]

        for label, sql in checks:
            try:
                self.cursor.execute(sql)
                val = self.cursor.fetchone()[0]
                print(f"{label:<28}: {val:,}")
            except Exception as exc:
                print(f"{label:<28}: (skipped) {exc}")

        # Dangling media pointers (xref rows referencing a missing media.media_id)
        dangling_sqls = [
            ("dangling indi_media_xref", """SELECT COUNT(*) FROM indi_media_xref x
                                           LEFT JOIN media m ON m.media_id = x.media_id
                                           WHERE m.media_id IS NULL"""),
            ("dangling fam_media_xref", """SELECT COUNT(*) FROM fam_media_xref x
                                          LEFT JOIN media m ON m.media_id = x.media_id
                                          WHERE m.media_id IS NULL"""),
            ("dangling event_media_xref", """SELECT COUNT(*) FROM event_media_xref x
                                            LEFT JOIN media m ON m.media_id = x.media_id
                                            WHERE m.media_id IS NULL"""),
        ]
        for label, sql in dangling_sqls:
            try:
                self.cursor.execute(sql)
                val = self.cursor.fetchone()[0]
                print(f"{label:<28}: {val:,}")
            except Exception as exc:
                print(f"{label:<28}: (skipped) {exc}")

    def close(self) -> None:
        """Close database resources (safe to call multiple times)."""
        # Some methods use self.cur, others self.cursor; handle both.
        for attr in ("cur", "cursor"):
            obj = getattr(self, attr, None)
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass
        conn = getattr(self, "conn", None)
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_FILE_DEFAULT = os.path.normpath(
    os.path.join(_SCRIPT_DIR, '..', 'config', 'website_config.ini')
)

def _read_ini_db_config(ini_path: str) -> dict:
    """Read [Database] section from an INI file and return a partial db_config dict.
    Only keys that are actually present in the file are returned, so other
    defaults are not overwritten by missing/blank values."""
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    result = {}
    if cfg.has_section("Database"):
        mapping = {"Host": "host", "User": "user", "Password": "password", "Database": "database"}
        for ini_key, dict_key in mapping.items():
            val = cfg.get("Database", ini_key, fallback=None)
            if val is not None:
                result[dict_key] = val
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import a GEDCOM file into MySQL using an event-centric schema."
    )
    parser.add_argument("gedcom_file", help="Path to GEDCOM file to import")
    parser.add_argument("--places-csv", default=None,
                        help="Reviewed places CSV from gedcom_places_build.py (adds structured place fields to MySQL)")
    parser.add_argument("--config", default=_CONFIG_FILE_DEFAULT,
                        help="INI file with [Database] credentials (default: ../config/website_config.ini relative to script)")
    parser.add_argument("--host", default=None, help="MySQL host (overrides config file)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("MYSQL_PORT", "3306")), help="MySQL port")
    parser.add_argument("--user", default=None, help="MySQL user (overrides config file)")
    parser.add_argument("--password", default=None, help="MySQL password (overrides config file)")
    parser.add_argument("--db", default=None, help="MySQL database name (overrides config file)")
    parser.add_argument("--clear", choices=["truncate", "delete", "none"],
                        default=os.environ.get("GEDCOM_CLEAR_MODE", "truncate"),
                        help="How to clear tables before import (default: truncate)")
    parser.add_argument("--chunk-size", type=int, default=int(os.environ.get("GEDCOM_CHUNK_SIZE", "5000")),
                        help="Batch size for executemany inserts (default: 5000)")
    parser.add_argument("--no-validate", action="store_true", help="Disable post-load validation checks")

    args = parser.parse_args()

    # Build db_config: INI file is the base, command-line args override,
    # environment variables fill any remaining gaps.
    ini_cfg = _read_ini_db_config(args.config)
    if ini_cfg:
        print(f"  Credentials loaded from: {args.config}")

    db_config = {
        "host":     args.host     or ini_cfg.get("host")     or os.environ.get("MYSQL_HOST",     "localhost"),
        "port":     args.port,
        "user":     args.user     or ini_cfg.get("user")     or os.environ.get("MYSQL_USER",     "root"),
        "password": args.password or ini_cfg.get("password") or os.environ.get("MYSQL_PASSWORD"),
        "database": args.db       or ini_cfg.get("database") or os.environ.get("MYSQL_DB",       "genealogy"),
    }

    if db_config["password"] is None:
        # Prompt only if no password was found anywhere
        db_config["password"] = getpass.getpass("MySQL password: ")

    importer = GedcomImporter(
        db_config,
        clear_mode=args.clear,
        chunk_size=args.chunk_size,
        validate_post_load=(not args.no_validate),
    )
    if args.places_csv:
        print(f"\nLoading places CSV: {args.places_csv}")
        importer.load_places_csv(args.places_csv)
    importer.parse_gedcom(args.gedcom_file)
    importer.write_to_database()
    importer.close()
if __name__ == '__main__':
    main()
    print('\n####################################################\n\n\n\n\n')