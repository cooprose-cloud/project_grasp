#!/usr/bin/env python3
"""
gedcom_select.py — pick individuals, sources, and/or media from searchable
browser tables, then save the removal list (ids.txt) or run the prune.

Standard library only — nothing to install. Opens a local page with three
tabs (Individuals / Sources / Media). Each is a searchable, click-to-select
table. For individuals you can also grow the selection along the family tree
(+ Ancestors / + Descendants) and Invert — the keep-a-branch workflow.

Actions:
  * Save to ids.txt  — write the current selection (any mix of individuals,
                       sources, media).
  * Save & Prune     — write ids.txt AND produce the trimmed GEDCOM, showing
                       the removal report in the page.
  * Cancel           — write nothing and shut the server down.

Removal is surgical: selected individuals are removed (and their emptied
families cleaned up); selected sources/media are removed and their citations
and links detached from everyone — people are never removed just for citing a
removed source. Orphaned sources/media are then pruned. All done by
gedcom_prune.py (imported), which stays usable on its own.

USAGE:
  python3 GRASP_System/scripts/gedcom_select.py GRASP_user/gedcoms/FINAL_ROSv_20260415_clean.ged

  --ids FILE      selection list to read/write (default: ids.txt)
  --output FILE   trimmed GEDCOM for "Save & Prune"
                  (default: <input name>.trimmed.ged next to the input)
  --keep-orphans          keep ALL sources & media even if unreferenced
  --keep-orphan-sources   prune orphaned media only; keep all sources
  --port N        local port (default 8765)
  --no-open       don't auto-open the browser
"""

import argparse
import json
import os
import sys
import threading
import webbrowser
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer

# Reuse the parsing + pruning engine from the pruner (same folder)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gedcom_prune import (read_gedcom_lines, index_records, person_name,
                          source_label, media_label, is_pointer, prune,
                          write_gedcom, require_distinct, classify_tokens,
                          even_type_value)

# Tags that live at level 1 under INDI/FAM but are NOT events/attributes
STRUCTURAL_TAGS = {
    "NAME", "SEX", "OBJE", "FAMS", "FAMC", "HUSB", "WIFE", "CHIL", "CHAN",
    "NOTE", "SOUR", "RIN", "RFN", "AFN", "RESN", "SUBM", "SUBN", "ANCI",
    "DESI", "ALIA", "ASSO", "REFN", "_PHOTO", "_UID",
}
FRIENDLY_EVENTS = {
    "BIRT": "Birth", "CHR": "Christening", "DEAT": "Death", "BURI": "Burial",
    "CREM": "Cremation", "ADOP": "Adoption", "BAPM": "Baptism", "CONF": "Confirmation",
    "FCOM": "First Communion", "ORDN": "Ordination", "NATU": "Naturalization",
    "EMIG": "Emigration", "IMMI": "Immigration", "CENS": "Census", "PROB": "Probate",
    "WILL": "Will", "GRAD": "Graduation", "RETI": "Retirement", "EVEN": "Event",
    "EDUC": "Education", "OCCU": "Occupation", "RELI": "Religion", "RESI": "Residence",
    "TITL": "Title", "PROP": "Property", "NATI": "Nationality", "DSCR": "Description",
    "ANUL": "Annulment", "DIV": "Divorce", "DIVF": "Divorce Filed", "ENGA": "Engagement",
    "MARB": "Marriage Banns", "MARC": "Marriage Contract", "MARR": "Marriage",
    "MARL": "Marriage License", "MARS": "Marriage Settlement", "_MILT": "Military",
}


def event_year(lines, rec, evt):
    inblk = False
    for idx in sorted(rec["block"]):
        lv, tag, rest, _ = lines[idx]
        if lv == 1 and tag == evt:
            inblk = True
            continue
        if inblk and lv == 1:
            inblk = False
        if inblk and lv == 2 and tag == "DATE":
            toks = rest.replace(",", " ").split()
            yrs = [t for t in toks if t.isdigit() and len(t) == 4]
            return yrs[-1] if yrs else rest
    return ""


def build_all(gedcom_path):
    """Return (individuals, sources, media, parents, children)."""
    lines = read_gedcom_lines(gedcom_path)
    recs = index_records(lines)

    people, sources, media = [], [], []
    fam = {}
    for xref, rec in recs.items():
        t = rec["type"]
        if t == "INDI":
            people.append({
                "id": xref, "name": person_name(lines, rec),
                "birth": event_year(lines, rec, "BIRT"),
                "death": event_year(lines, rec, "DEAT"),
            })
        elif t == "SOUR":
            sources.append({"id": xref, "label": source_label(lines, rec) or "(untitled source)"})
        elif t == "OBJE":
            media.append({"id": xref, "label": media_label(lines, rec) or "(untitled media)"})
        elif t == "FAM":
            h = w = None
            ch = []
            for idx in sorted(rec["block"]):
                lv, tag, rest, _ = lines[idx]
                if lv == 1 and tag == "HUSB" and is_pointer(rest):
                    h = rest
                elif lv == 1 and tag == "WIFE" and is_pointer(rest):
                    w = rest
                elif lv == 1 and tag == "CHIL" and is_pointer(rest):
                    ch.append(rest)
            fam[xref] = (h, w, ch)

    parents = defaultdict(set)
    children = defaultdict(set)
    for xref, rec in recs.items():
        if rec["type"] != "INDI":
            continue
        for idx in sorted(rec["block"]):
            lv, tag, rest, _ = lines[idx]
            if lv == 1 and tag == "FAMC" and is_pointer(rest) and rest in fam:
                h, w, _ch = fam[rest]
                for p in (h, w):
                    if p:
                        parents[xref].add(p)
            elif lv == 1 and tag == "FAMS" and is_pointer(rest) and rest in fam:
                _h, _w, ch = fam[rest]
                for c in ch:
                    children[xref].add(c)

    # Media attached under each source's citations (or directly on the source),
    # so we can offer "remove a source and its media" in one step.
    src_media = defaultdict(set)
    for idx, (lv, tag, rest, _) in enumerate(lines):
        if lv and lv >= 1 and tag == "OBJE" and is_pointer(rest) \
                and rest in recs and recs[rest]["type"] == "OBJE":
            cur = lv
            j = idx - 1
            sid = owner0 = None
            while j >= 0:
                plv, ptag, prest, _ = lines[j]
                if plv is not None and plv < cur:
                    if ptag == "SOUR" and is_pointer(prest):
                        sid = prest
                        break
                    if plv == 0:
                        if prest == "SOUR" and is_pointer(ptag):
                            owner0 = ptag
                        break
                    cur = plv
                j -= 1
            source = sid or owner0
            if source and source in recs and recs[source]["type"] == "SOUR":
                src_media[source].add(rest)

    # Event types present under INDI/FAM (for the Event types tab)
    tag_counts = defaultdict(int)
    even_counts = defaultdict(int)
    even_untyped = 0
    cur_owner = None
    for idx, (lv, tag, rest, _) in enumerate(lines):
        if lv == 0:
            cur_owner = recs.get(tag, {}).get("type") if is_pointer(tag) else None
        elif lv == 1 and cur_owner in ("INDI", "FAM"):
            if tag == "EVEN":
                tv = even_type_value(lines, idx)
                if tv:
                    even_counts[tv] += 1
                else:
                    even_untyped += 1
            elif tag not in STRUCTURAL_TAGS:
                tag_counts[tag] += 1
    event_types = []
    for tag, c in tag_counts.items():
        event_types.append({"id": f"EVENT:{tag}",
                             "label": f"{tag} — {FRIENDLY_EVENTS.get(tag, 'event/attribute')}",
                             "count": c})
    for tv, c in even_counts.items():
        event_types.append({"id": f"EVENTTYPE:{tv}", "label": f"EVEN — {tv}", "count": c})
    if even_untyped:
        event_types.append({"id": "EVENT:EVEN",
                            "label": "EVEN — generic (no custom type)", "count": even_untyped})
    event_types.sort(key=lambda r: -r["count"])

    def sk(p):
        parts = p["name"].split()
        return (parts[-1].lower() if parts else "", p["name"].lower())
    people.sort(key=sk)
    sources.sort(key=lambda r: (r["label"].lower(), r["id"]))
    media.sort(key=lambda r: (r["label"].lower(), r["id"]))
    return (people, sources, media,
            {k: list(v) for k, v in parents.items()},
            {k: list(v) for k, v in children.items()},
            {k: list(v) for k, v in src_media.items()},
            event_types)


def _closure(seeds, adj):
    seeds = set(seeds)
    seen = set()
    stack = list(seeds)
    while stack:
        x = stack.pop()
        for y in adj.get(x, ()):
            if y not in seen and y not in seeds:
                seen.add(y)
                stack.append(y)
    return seen


def load_existing_ids(ids_path):
    ids = set()
    if not os.path.isfile(ids_path):
        return ids
    with open(ids_path, encoding="utf-8-sig", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            token = line.replace(",", " ").split()[0]
            if not token.startswith("@"):
                token = "@" + token
            if not token.endswith("@"):
                token = token + "@"
            ids.add(token)
    return ids


def write_ids_file(ids_path, ids, order, label_map):
    ids = sorted(set(ids), key=lambda x: order.get(x, 1e9))
    with open(ids_path, "w", encoding="utf-8") as fh:
        fh.write("# gedcom_prune removal list — written by gedcom_select.py\n")
        fh.write("# Individuals/sources/media by @ID@; event lines like 'EVENT:RESI' / 'EVENTTYPE:Arrival'.\n")
        for i in ids:
            if str(i).upper().startswith("EVENT"):
                fh.write(f"{i}\n")
            else:
                fh.write(f"{i}   {label_map.get(i,'')}\n")
    return ids


PAGE = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Select records to remove</title>
<style>
  :root{--navy:#16213E;--gold:#D4AF37;}
  *{box-sizing:border-box;}
  body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#f5f3ee;color:#1c1c1c;}
  header{position:sticky;top:0;background:var(--navy);color:#fff;padding:12px 20px;box-shadow:0 2px 6px rgba(0,0,0,.2);z-index:10;}
  header h1{margin:0 0 8px;font-size:1.15em;}
  .tabs{display:flex;gap:6px;margin-bottom:8px;}
  .tab{background:#31406a;color:#dfe4f0;border:none;padding:7px 14px;border-radius:6px 6px 0 0;cursor:pointer;font-weight:600;}
  .tab.active{background:#f5f3ee;color:#16213E;}
  .bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
  #q{flex:1;min-width:200px;padding:9px 12px;border-radius:6px;border:1px solid #ccc;font-size:1em;}
  button{background:var(--gold);color:#1c1c1c;border:none;padding:9px 13px;border-radius:6px;font-weight:600;cursor:pointer;font-size:.92em;}
  button.ghost{background:#e7e2d6;}
  button.warn{background:#c9c2b4;}
  button:disabled{opacity:.5;cursor:default;}
  .count{color:#fff;font-weight:600;white-space:nowrap;}
  #msg{padding:12px 20px;display:none;color:#fff;}
  #msg h3{margin:0 0 6px;}
  #msg table{border-collapse:collapse;margin-top:6px;background:rgba(255,255,255,.12);}
  #msg td{padding:3px 12px;border:1px solid rgba(255,255,255,.25);}
  .wrap{padding:14px 20px 60px;}
  table.grid{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.15);}
  table.grid th,table.grid td{padding:7px 10px;text-align:left;border-bottom:1px solid #eee;font-size:.93em;}
  table.grid th{position:sticky;top:118px;background:#efe9db;cursor:pointer;user-select:none;}
  th.id,td.id{font-family:ui-monospace,Menlo,monospace;color:#555;white-space:nowrap;}
  td.lbl{word-break:break-word;}
  tr.sel{background:#fff7db;}
  table.grid tbody tr:hover{background:#f0ece1;}
  td.chk,th.chk{width:34px;text-align:center;}
  .yr{color:#666;width:70px;}
  table.grid tbody tr{cursor:pointer;}
</style></head>
<body>
<header>
  <h1>Select records to remove</h1>
  <div class="tabs">
    <button class="tab active" data-tab="individuals" id="tab-individuals">Individuals (<span id="ci">0</span>)</button>
    <button class="tab" data-tab="sources" id="tab-sources">Sources (<span id="cs">0</span>)</button>
    <button class="tab" data-tab="media" id="tab-media">Media (<span id="cm">0</span>)</button>
    <button class="tab" data-tab="events" id="tab-events">Event types (<span id="ce">0</span>)</button>
  </div>
  <div class="bar">
    <input id="q" placeholder="Search…" autocomplete="off">
    <button class="ghost" id="selshown">Select all shown</button>
    <button class="ghost" id="anc">+ Ancestors</button>
    <button class="ghost" id="desc">+ Descendants</button>
    <button class="ghost" id="srcmedia">+ Media of selected sources</button>
    <button class="ghost" id="invert">Invert (this tab)</button>
    <button class="ghost" id="clear">Clear all</button>
    <span class="count" id="sel">0 selected</span>
    <button id="save">Save to ids.txt</button>
    <button id="prune">Save &amp; Prune</button>
    <button class="warn" id="cancel">Cancel</button>
  </div>
</header>
<div id="msg"></div>
<div class="wrap">
<table class="grid">
  <thead id="thead"></thead>
  <tbody id="rows"></tbody>
</table>
</div>
<script>
const DATA = {individuals: __INDIV__, sources: __SOURCES__, media: __MEDIA__, events: __EVENTS__};
const PRE = new Set(__PRESEL__);
const selected = new Set(PRE);
const IDSET = {ind:new Set(DATA.individuals.map(x=>x.id)),
               src:new Set(DATA.sources.map(x=>x.id)),
               med:new Set(DATA.media.map(x=>x.id)),
               evt:new Set(DATA.events.map(x=>x.id))};
let tab='individuals', sortK='name', sortDir=1, filter='';

const rows=document.getElementById('rows');
const thead=document.getElementById('thead');
const q=document.getElementById('q');
const msg=document.getElementById('msg');

function surnameKey(p){const n=(p.name||'');const s=n.split(' ').slice(-1)[0]||'';return s.toLowerCase()+'|'+n.toLowerCase();}
function label(x){return x.name!==undefined?x.name:x.label;}
function shown(){
  const list=DATA[tab]; const f=filter.trim().toLowerCase();
  let r=list.filter(x=> !f || label(x).toLowerCase().includes(f) || x.id.toLowerCase().includes(f));
  r.sort((a,b)=>{
    if(sortK==='count') return (a.count-b.count)*sortDir;
    let ka,kb;
    if(sortK==='name'){ka=surnameKey(a);kb=surnameKey(b);}
    else{ka=(a[sortK]||'').toString().toLowerCase();kb=(b[sortK]||'').toString().toLowerCase();}
    return ka<kb?-sortDir:ka>kb?sortDir:0;
  });
  return r;
}
function headHtml(){
  if(tab==='individuals') return `<tr><th class="chk"></th><th data-k="name">Name</th><th class="id" data-k="id">ID</th><th data-k="birth">Birth</th><th data-k="death">Death</th></tr>`;
  if(tab==='events') return `<tr><th class="chk"></th><th data-k="label">Event type</th><th data-k="count">Count</th></tr>`;
  const lab = tab==='sources' ? 'Source title' : 'Media file';
  return `<tr><th class="chk"></th><th data-k="label">${lab}</th><th class="id" data-k="id">ID</th></tr>`;
}
function rowHtml(x){
  const c=`<td class="chk"><input type="checkbox" ${selected.has(x.id)?'checked':''}></td>`;
  if(tab==='individuals') return c+`<td>${esc(x.name)}</td><td class="id">${x.id}</td><td class="yr">${x.birth||''}</td><td class="yr">${x.death||''}</td>`;
  if(tab==='events') return c+`<td class="lbl">${esc(x.label)}</td><td class="yr">${x.count}</td>`;
  return c+`<td class="lbl">${esc(x.label)}</td><td class="id">${x.id}</td>`;
}
function counts(){let ci=0,cs=0,cm=0,ce=0;for(const id of selected){if(IDSET.ind.has(id))ci++;else if(IDSET.src.has(id))cs++;else if(IDSET.med.has(id))cm++;else if(IDSET.evt.has(id))ce++;}return{ci,cs,cm,ce};}
function render(){
  thead.innerHTML=headHtml();
  thead.querySelectorAll('th[data-k]').forEach(th=>{th.onclick=()=>{const k=th.dataset.k;if(sortK===k)sortDir*=-1;else{sortK=k;sortDir=1;}render();};});
  const list=shown(); rows.innerHTML='';
  const frag=document.createDocumentFragment();
  for(const x of list){
    const tr=document.createElement('tr');
    tr.className=selected.has(x.id)?'sel':'';
    tr.innerHTML=rowHtml(x);
    tr.onclick=(e)=>{if(e.target.tagName!=='INPUT')toggle(x.id);};
    tr.querySelector('input').onchange=()=>toggle(x.id);
    frag.appendChild(tr);
  }
  rows.appendChild(frag);
  const c=counts();
  document.getElementById('ci').textContent=c.ci;
  document.getElementById('cs').textContent=c.cs;
  document.getElementById('cm').textContent=c.cm;
  document.getElementById('ce').textContent=c.ce;
  document.getElementById('sel').textContent=`${selected.size} selected (${c.ci} people, ${c.cs} sources, ${c.cm} media, ${c.ce} event types)`;
  const showKeep = tab==='individuals' ? 'inline-block':'none';
  document.getElementById('anc').style.display=showKeep;
  document.getElementById('desc').style.display=showKeep;
  document.getElementById('srcmedia').style.display = tab==='sources' ? 'inline-block':'none';
}
function toggle(id){selected.has(id)?selected.delete(id):selected.add(id);render();}
function esc(s){return (s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}

q.oninput=()=>{filter=q.value;render();};
document.querySelectorAll('.tab').forEach(t=>{
  t.onclick=()=>{document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    t.classList.add('active'); tab=t.dataset.tab;
    if(tab==='events'){sortK='count';sortDir=-1;}else{sortK=(tab==='individuals'?'name':'label');sortDir=1;}
    render();};
});
document.getElementById('selshown').onclick=()=>{shown().forEach(x=>selected.add(x.id));render();};
document.getElementById('invert').onclick=()=>{for(const x of DATA[tab]){selected.has(x.id)?selected.delete(x.id):selected.add(x.id);}render();};
document.getElementById('clear').onclick=()=>{selected.clear();render();};

function banner(ok,html){msg.style.display='block';msg.style.background=ok?'#2E7D32':'#b00020';msg.innerHTML=html;window.scrollTo(0,0);}
function setBusy(b){['save','prune','cancel'].forEach(id=>document.getElementById(id).disabled=b);}

async function expand(mode){
  const seeds=[...selected].filter(id=>IDSET.ind.has(id));
  if(seeds.length===0){banner(false,'Select at least one individual first — the root of the branch to keep.');return;}
  setBusy(true);
  try{
    const r=await fetch('/expand',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:seeds,mode})});
    const j=await r.json();
    if(!j.ok){banner(false,'Error: '+(j.error||'expand failed'));return;}
    j.ids.forEach(id=>selected.add(id)); render();
    banner(true,`Added ${j.added} ${mode}. Branch to keep now has ${counts().ci} people. `+
               `Next: <b>Invert (this tab)</b> to turn it into the removal list, then Save &amp; Prune.`);
  }finally{setBusy(false);}
}
document.getElementById('anc').onclick=()=>expand('ancestors');
document.getElementById('desc').onclick=()=>expand('descendants');

document.getElementById('srcmedia').onclick=async()=>{
  const sel=[...selected].filter(id=>IDSET.src.has(id));
  if(sel.length===0){banner(false,'Select at least one source first (on the Sources tab).');return;}
  setBusy(true);
  try{
    const r=await fetch('/source-media',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:sel})});
    const j=await r.json();
    if(!j.ok){banner(false,'Error: '+(j.error||'lookup failed'));return;}
    const before=counts().cm;
    j.media.forEach(id=>selected.add(id)); render();
    const added=counts().cm-before;
    banner(true,`Added ${added} media attached to ${j.sources} selected source(s). `+
               `They'll be removed together on Save &amp; Prune. `+
               (added?`(Reminder: a media item is deleted everywhere it appears.)`:`(These sources had no attached media.)`));
  }finally{setBusy(false);}
};

document.getElementById('save').onclick=async()=>{
  setBusy(true);
  try{
    const r=await fetch('/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:[...selected]})});
    const j=await r.json();
    banner(j.ok, j.ok?`Saved ${j.count} record(s) to <b>${j.path}</b>.`:('Error: '+(j.error||'could not write file')));
  }finally{setBusy(false);}
};

document.getElementById('prune').onclick=async()=>{
  if(selected.size===0){banner(false,'Nothing selected to remove.');return;}
  const c=counts();
  if(!confirm(`Remove ${c.ci} individual(s), ${c.cs} source(s), ${c.cm} media, ${c.ce} event type(s) and write the trimmed GEDCOM?`))return;
  setBusy(true);
  try{
    const r=await fetch('/prune',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:[...selected]})});
    const j=await r.json();
    if(!j.ok){banner(false,'Error: '+(j.error||'prune failed'));return;}
    const s=j.report;
    banner(true,
      `<h3>Done — trimmed GEDCOM written</h3>`+
      `<div>Output: <b>${j.output}</b></div>`+
      `<div>List saved to: ${j.ids_path}</div>`+
      `<table>`+
      `<tr><td>Individuals removed</td><td><b>${s.individuals}</b></td></tr>`+
      `<tr><td>Sources removed (selected)</td><td>${s.sources_removed}</td></tr>`+
      `<tr><td>Media removed (selected)</td><td>${s.media_removed}</td></tr>`+
      `<tr><td>Events removed</td><td>${s.events_removed}</td></tr>`+
      `<tr><td>Families deleted (emptied)</td><td>${s.families_deleted}</td></tr>`+
      `<tr><td>Families kept but detached</td><td>${s.families_modified}</td></tr>`+
      `<tr><td>Sources pruned (orphaned)</td><td>${s.sources_pruned}</td></tr>`+
      `<tr><td>Media pruned (orphaned)</td><td>${s.media_pruned}</td></tr>`+
      `</table>`+
      (s.skipped?`<div style="margin-top:6px">Note: ${s.skipped} listed id(s) were skipped (not found / not removable).</div>`:``));
  }finally{setBusy(false);}
};

document.getElementById('cancel').onclick=async()=>{
  if(!confirm('Cancel without saving? This also shuts the selector down.'))return;
  setBusy(true);
  try{await fetch('/cancel',{method:'POST'});}catch(e){}
  banner(false,'Cancelled — nothing was written. The selector has stopped; you can close this tab.');
};

render();
</script>
</body></html>"""


def make_handler(cfg):
    presel = load_existing_ids(cfg["ids_path"])
    all_records = cfg["people"] + cfg["sources"] + cfg["media"]
    order = {r["id"]: i for i, r in enumerate(all_records)}
    label_map = {}
    for p in cfg["people"]:
        label_map[p["id"]] = p["name"]
    for s in cfg["sources"] + cfg["media"]:
        label_map[s["id"]] = s["label"]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="text/html; charset=utf-8"):
            data = body.encode("utf-8") if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _json(self, obj):
            self._send(200, json.dumps(obj), "application/json")

        def _body(self):
            length = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(length) or b"{}")

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                html = (PAGE
                        .replace("__INDIV__", json.dumps(cfg["people"]))
                        .replace("__SOURCES__", json.dumps(cfg["sources"]))
                        .replace("__MEDIA__", json.dumps(cfg["media"]))
                        .replace("__EVENTS__", json.dumps(cfg["events"]))
                        .replace("__PRESEL__", json.dumps(sorted(presel))))
                self._send(200, html)
            else:
                self._send(404, "not found", "text/plain")

        def do_POST(self):
            try:
                if self.path == "/save":
                    ids = self._body().get("ids", [])
                    written = write_ids_file(cfg["ids_path"], ids, order, label_map)
                    print(f"  ✓ Saved {len(written)} record(s) to {os.path.abspath(cfg['ids_path'])}",
                          flush=True)
                    self._json({"ok": True, "count": len(written),
                                "path": os.path.abspath(cfg["ids_path"])})

                elif self.path == "/prune":
                    ids = self._body().get("ids", [])
                    write_ids_file(cfg["ids_path"], ids, order, label_map)
                    rec_ids, ev_tags, ev_types = classify_tokens(ids)
                    lines = read_gedcom_lines(cfg["gedcom"])
                    alive, stats = prune(lines, rec_ids,
                                         prune_sources=cfg["prune_sources"],
                                         prune_media=cfg["prune_media"],
                                         remove_event_tags=ev_tags,
                                         remove_even_types=ev_types)
                    write_gedcom(lines, alive, cfg["output"])
                    report = {
                        "individuals": len(stats["removed_individuals"]),
                        "sources_removed": len(stats["removed_sources"]),
                        "media_removed": len(stats["removed_media"]),
                        "events_removed": stats["events_removed"],
                        "families_deleted": len(stats["families_deleted"]),
                        "families_modified": len(stats["families_modified"]),
                        "sources_pruned": len(stats["sources_pruned"]),
                        "media_pruned": len(stats["media_pruned"]),
                        "skipped": len(stats["missing_ids"]) + len(stats["wrong_type_ids"]),
                    }
                    print(f"  ✓ Pruned: removed {report['individuals']} individual(s), "
                          f"{report['sources_removed']} source(s), {report['media_removed']} media, "
                          f"{report['events_removed']} event(s) -> {os.path.abspath(cfg['output'])}",
                          flush=True)
                    self._json({"ok": True, "output": os.path.abspath(cfg["output"]),
                                "ids_path": os.path.abspath(cfg["ids_path"]),
                                "report": report})

                elif self.path == "/expand":
                    payload = self._body()
                    seeds = list(payload.get("ids", []))
                    mode = payload.get("mode", "")
                    adj = cfg["parents"] if mode == "ancestors" else cfg["children"]
                    rel = _closure(seeds, adj)
                    allids = sorted(set(seeds) | rel, key=lambda x: order.get(x, 1e9))
                    self._json({"ok": True, "ids": allids, "added": len(rel - set(seeds))})

                elif self.path == "/source-media":
                    sel = self._body().get("ids", [])
                    med = set()
                    nsrc = 0
                    for sid in sel:
                        if sid in cfg["source_media"]:
                            nsrc += 1
                            med.update(cfg["source_media"][sid])
                    self._json({"ok": True, "media": sorted(med), "sources": nsrc})

                elif self.path == "/cancel":
                    self._json({"ok": True})
                    threading.Thread(target=self.server.shutdown, daemon=True).start()

                else:
                    self._send(404, '{"ok":false}', "application/json")
            except Exception as e:
                self._json({"ok": False, "error": str(e)})

    return Handler


def main():
    ap = argparse.ArgumentParser(
        description="Pick individuals/sources/media in a browser; save or prune.")
    ap.add_argument("gedcom", help="GEDCOM file to list records from")
    ap.add_argument("--ids", default="ids.txt",
                    help="selection list to read/write (default ids.txt)")
    ap.add_argument("--output", default=None,
                    help="trimmed GEDCOM for Save & Prune (default <input>.trimmed.ged)")
    ap.add_argument("--keep-orphans", action="store_true")
    ap.add_argument("--keep-orphan-sources", action="store_true")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-open", action="store_true")
    args = ap.parse_args()

    if not os.path.isfile(args.gedcom):
        sys.exit(f"ERROR: GEDCOM not found: {args.gedcom}")
    if args.output is None:
        root, ext = os.path.splitext(args.gedcom)
        args.output = root + ".trimmed" + (ext or ".ged")
    require_distinct(args.gedcom, args.output)

    people, sources, media, parents, children, source_media, events = build_all(args.gedcom)
    cfg = {
        "people": people, "sources": sources, "media": media, "events": events,
        "parents": parents, "children": children, "source_media": source_media,
        "ids_path": args.ids, "gedcom": args.gedcom, "output": args.output,
        "prune_sources": not (args.keep_orphans or args.keep_orphan_sources),
        "prune_media": not args.keep_orphans,
    }

    httpd = HTTPServer(("127.0.0.1", args.port), make_handler(cfg))
    url = f"http://127.0.0.1:{args.port}/"
    print(f"Loaded {len(people)} individuals, {len(sources)} sources, {len(media)} media, "
          f"{len(events)} event types from {args.gedcom}")
    print(f"Selector running at {url}")
    print(f"  Save to ids.txt -> {os.path.abspath(args.ids)}")
    print(f"  Save & Prune    -> {os.path.abspath(args.output)}")
    print("Leave this running while you pick; Cancel in the page (or Ctrl+C here) stops it.")
    if not args.no_open:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
