#!/usr/bin/env python3
"""
gedcom_select.py — pick individuals from a searchable table in your browser,
then either save the list (ids.txt) or run the prune right away.

Runs a tiny local web server (standard library only — nothing to install),
opens a page listing everyone in the GEDCOM with a checkbox, a live search
box, sortable columns, "Select all shown", and "Invert selection". Three
actions:

  * Save to ids.txt  — write just the selection list.
  * Save & Prune     — write ids.txt AND produce the trimmed GEDCOM, showing
                       the removal report right in the page.
  * Cancel           — write nothing and shut the server down.

The pruning itself is done by gedcom_prune.py (imported), so the tested engine
stays reusable on its own.

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
                          is_pointer, prune, write_gedcom, require_distinct)


def event_year(lines, rec, evt):
    """Best-effort 4-digit year for a life event (BIRT/DEAT)."""
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


def build_individuals(gedcom_path):
    lines = read_gedcom_lines(gedcom_path)
    recs = index_records(lines)
    people = []
    for xref, rec in recs.items():
        if rec["type"] != "INDI":
            continue
        people.append({
            "id": xref,
            "name": person_name(lines, rec),
            "birth": event_year(lines, rec, "BIRT"),
            "death": event_year(lines, rec, "DEAT"),
        })

    def sort_key(p):
        parts = p["name"].split()
        return (parts[-1].lower() if parts else "", p["name"].lower())

    people.sort(key=sort_key)
    return people


def build_relationships(gedcom_path):
    """Return (parents, children) maps: id -> list of parent/child ids."""
    lines = read_gedcom_lines(gedcom_path)
    recs = index_records(lines)
    fam = {}
    for xref, rec in recs.items():
        if rec["type"] != "FAM":
            continue
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
    return ({k: list(v) for k, v in parents.items()},
            {k: list(v) for k, v in children.items()})


def _closure(seeds, adj):
    """All ids reachable from seeds following adjacency map adj (excludes seeds)."""
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


def write_ids_file(ids_path, ids, order, name_map):
    ids = sorted(set(ids), key=lambda x: order.get(x, 1e9))
    with open(ids_path, "w", encoding="utf-8") as fh:
        fh.write("# gedcom_prune removal list — written by gedcom_select.py\n")
        fh.write("# One individual per line; text after the ID is just a note.\n")
        for i in ids:
            fh.write(f"{i}   {name_map.get(i,'')}\n")
    return ids


PAGE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Select individuals to remove</title>
<style>
  :root{--navy:#16213E;--gold:#D4AF37;}
  *{box-sizing:border-box;}
  body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
       margin:0;background:#f5f3ee;color:#1c1c1c;}
  header{position:sticky;top:0;background:var(--navy);color:#fff;padding:14px 20px;
         box-shadow:0 2px 6px rgba(0,0,0,.2);z-index:10;}
  header h1{margin:0 0 8px;font-size:1.2em;}
  .bar{display:flex;gap:12px;align-items:center;flex-wrap:wrap;}
  #q{flex:1;min-width:200px;padding:9px 12px;border-radius:6px;border:1px solid #ccc;font-size:1em;}
  button{background:var(--gold);color:#1c1c1c;border:none;padding:9px 14px;border-radius:6px;
         font-weight:600;cursor:pointer;font-size:.95em;}
  button.ghost{background:#e7e2d6;}
  button.warn{background:#c9c2b4;}
  button:disabled{opacity:.5;cursor:default;}
  .count{color:#fff;font-weight:600;white-space:nowrap;}
  #msg{padding:12px 20px;display:none;color:#fff;}
  #msg h3{margin:0 0 6px;}
  #msg table{border-collapse:collapse;margin-top:6px;background:rgba(255,255,255,.12);}
  #msg td{padding:3px 12px;border:1px solid rgba(255,255,255,.25);}
  .wrap{padding:14px 20px 60px;}
  table.people{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.15);}
  table.people th,table.people td{padding:7px 10px;text-align:left;border-bottom:1px solid #eee;font-size:.93em;}
  table.people th{position:sticky;top:100px;background:#efe9db;cursor:pointer;user-select:none;}
  th.id,td.id{font-family:ui-monospace,Menlo,monospace;color:#555;}
  tr.sel{background:#fff7db;}
  table.people tbody tr:hover{background:#f0ece1;}
  td.chk,th.chk{width:34px;text-align:center;}
  .yr{color:#666;width:70px;}
  table.people tbody tr{cursor:pointer;}
</style></head>
<body>
<header>
  <h1>Select individuals to remove</h1>
  <div class="bar">
    <input id="q" placeholder="Search by name or ID…" autocomplete="off">
    <button class="ghost" id="selshown">Select all shown</button>
    <button class="ghost" id="anc">+ Ancestors of selected</button>
    <button class="ghost" id="desc">+ Descendants of selected</button>
    <button class="ghost" id="invert">Invert selection</button>
    <button class="ghost" id="clear">Clear all</button>
    <span class="count"><span id="n">0</span> selected</span>
    <button id="save">Save to ids.txt</button>
    <button id="prune">Save &amp; Prune</button>
    <button class="warn" id="cancel">Cancel</button>
  </div>
</header>
<div id="msg"></div>
<div class="wrap">
<table class="people">
  <thead><tr>
    <th class="chk"></th>
    <th data-k="name">Name</th>
    <th class="id" data-k="id">ID</th>
    <th data-k="birth">Birth</th>
    <th data-k="death">Death</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
</div>
<script>
const PEOPLE = __DATA__;
const PRE = new Set(__PRESEL__);
const selected = new Set(PRE);
let sortK='name', sortDir=1, filter='';

const rows=document.getElementById('rows');
const nEl=document.getElementById('n');
const q=document.getElementById('q');
const msg=document.getElementById('msg');

function shown(){
  const f=filter.trim().toLowerCase();
  let list=PEOPLE.filter(p=> !f || p.name.toLowerCase().includes(f) || p.id.toLowerCase().includes(f));
  list.sort((a,b)=>{
    let x=(a[sortK]||'').toString().toLowerCase(), y=(b[sortK]||'').toString().toLowerCase();
    if(sortK==='name'){x=a.name.split(' ').slice(-1)[0].toLowerCase()+a.name.toLowerCase();
                       y=b.name.split(' ').slice(-1)[0].toLowerCase()+b.name.toLowerCase();}
    return x<y?-sortDir:x>y?sortDir:0;
  });
  return list;
}
function render(){
  const list=shown();
  rows.innerHTML='';
  const frag=document.createDocumentFragment();
  for(const p of list){
    const tr=document.createElement('tr');
    tr.className=selected.has(p.id)?'sel':'';
    tr.innerHTML=`<td class="chk"><input type="checkbox" ${selected.has(p.id)?'checked':''}></td>`+
      `<td>${esc(p.name)}</td><td class="id">${p.id}</td>`+
      `<td class="yr">${p.birth||''}</td><td class="yr">${p.death||''}</td>`;
    tr.onclick=(e)=>{ if(e.target.tagName!=='INPUT'){toggle(p.id);} };
    tr.querySelector('input').onchange=()=>toggle(p.id);
    frag.appendChild(tr);
  }
  rows.appendChild(frag);
  nEl.textContent=selected.size;
}
function toggle(id){ selected.has(id)?selected.delete(id):selected.add(id); render(); }
function esc(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}

q.oninput=()=>{filter=q.value;render();};
document.querySelectorAll('th[data-k]').forEach(th=>{
  th.onclick=()=>{const k=th.dataset.k; if(sortK===k)sortDir*=-1; else{sortK=k;sortDir=1;} render();};
});
document.getElementById('selshown').onclick=()=>{shown().forEach(p=>selected.add(p.id));render();};
async function expand(mode){
  if(selected.size===0){ banner(false,'Select at least one person first — the root of the branch to keep.'); return; }
  setBusy(true);
  try{
    const r=await fetch('/expand',{method:'POST',headers:{'Content-Type':'application/json'},
                                  body:JSON.stringify({ids:[...selected],mode})});
    const j=await r.json();
    if(!j.ok){ banner(false,'Error: '+(j.error||'expand failed')); return; }
    selected.clear(); j.ids.forEach(id=>selected.add(id)); render();
    banner(true, `Added ${j.added} ${mode}. ${selected.size} people now selected (the branch to keep). `+
                 `Next: click <b>Invert selection</b> so everyone else becomes the removal list, then Save &amp; Prune.`);
  }finally{ setBusy(false); }
}
document.getElementById('anc').onclick=()=>expand('ancestors');
document.getElementById('desc').onclick=()=>expand('descendants');
document.getElementById('invert').onclick=()=>{
  const next=new Set();
  for(const p of PEOPLE){ if(!selected.has(p.id)) next.add(p.id); }
  selected.clear(); next.forEach(id=>selected.add(id)); render();
};
document.getElementById('clear').onclick=()=>{selected.clear();render();};

function banner(ok, html){
  msg.style.display='block';
  msg.style.background = ok ? '#2E7D32' : '#b00020';
  msg.innerHTML=html;
  window.scrollTo(0,0);
}
function setBusy(b){ ['save','prune','cancel'].forEach(id=>document.getElementById(id).disabled=b); }

document.getElementById('save').onclick=async()=>{
  setBusy(true);
  try{
    const r=await fetch('/save',{method:'POST',headers:{'Content-Type':'application/json'},
                                body:JSON.stringify({ids:[...selected]})});
    const j=await r.json();
    banner(j.ok, j.ok ? `Saved ${j.count} individual(s) to <b>${j.path}</b>.`
                      : ('Error: '+(j.error||'could not write file')));
  }finally{ setBusy(false); }
};

document.getElementById('prune').onclick=async()=>{
  if(selected.size===0){ banner(false,'Nothing selected to remove.'); return; }
  if(!confirm(`Remove ${selected.size} individual(s) and write the trimmed GEDCOM?`)) return;
  setBusy(true);
  try{
    const r=await fetch('/prune',{method:'POST',headers:{'Content-Type':'application/json'},
                                 body:JSON.stringify({ids:[...selected]})});
    const j=await r.json();
    if(!j.ok){ banner(false,'Error: '+(j.error||'prune failed')); return; }
    const s=j.report;
    banner(true,
      `<h3>Done — trimmed GEDCOM written</h3>`+
      `<div>Output: <b>${j.output}</b></div>`+
      `<div>List saved to: ${j.ids_path}</div>`+
      `<table>`+
      `<tr><td>Individuals removed</td><td><b>${s.individuals}</b></td></tr>`+
      `<tr><td>Families deleted (emptied)</td><td>${s.families_deleted}</td></tr>`+
      `<tr><td>Families kept but detached</td><td>${s.families_modified}</td></tr>`+
      `<tr><td>Sources pruned (orphaned)</td><td>${s.sources_pruned}</td></tr>`+
      `<tr><td>Media pruned (orphaned)</td><td>${s.media_pruned}</td></tr>`+
      `</table>`+
      (s.skipped ? `<div style="margin-top:6px">Note: ${s.skipped} listed id(s) were skipped (not found / not an individual).</div>` : ``));
  }finally{ setBusy(false); }
};

document.getElementById('cancel').onclick=async()=>{
  if(!confirm('Cancel without saving? This also shuts the selector down.')) return;
  setBusy(true);
  try{ await fetch('/cancel',{method:'POST'}); }catch(e){}
  banner(false,'Cancelled — nothing was written. The selector has stopped; you can close this tab.');
};

render();
</script>
</body></html>"""


def make_handler(cfg):
    presel = load_existing_ids(cfg["ids_path"])
    order = {p["id"]: i for i, p in enumerate(cfg["people"])}

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

        def _read_ids(self):
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length) or b"{}")
            return payload.get("ids", [])

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                html = (PAGE
                        .replace("__DATA__", json.dumps(cfg["people"]))
                        .replace("__PRESEL__", json.dumps(sorted(presel))))
                self._send(200, html)
            else:
                self._send(404, "not found", "text/plain")

        def do_POST(self):
            try:
                if self.path == "/save":
                    ids = self._read_ids()
                    written = write_ids_file(cfg["ids_path"], ids, order, cfg["name_map"])
                    print(f"  ✓ Saved {len(written)} id(s) to {os.path.abspath(cfg['ids_path'])}",
                          flush=True)
                    self._json({"ok": True, "count": len(written),
                                "path": os.path.abspath(cfg["ids_path"])})

                elif self.path == "/prune":
                    ids = self._read_ids()
                    write_ids_file(cfg["ids_path"], ids, order, cfg["name_map"])
                    lines = read_gedcom_lines(cfg["gedcom"])
                    alive, stats = prune(lines, list(ids),
                                         prune_sources=cfg["prune_sources"],
                                         prune_media=cfg["prune_media"])
                    write_gedcom(lines, alive, cfg["output"])
                    kept_indi = sum(1 for p in cfg["people"]
                                    if p["id"] not in set(rid for rid, _ in stats["removed_individuals"]))
                    print(f"  ✓ Pruned: removed {len(stats['removed_individuals'])} "
                          f"individual(s), {kept_indi} kept -> {os.path.abspath(cfg['output'])}",
                          flush=True)
                    report = {
                        "individuals": len(stats["removed_individuals"]),
                        "families_deleted": len(stats["families_deleted"]),
                        "families_modified": len(stats["families_modified"]),
                        "sources_pruned": len(stats["sources_pruned"]),
                        "media_pruned": len(stats["media_pruned"]),
                        "skipped": len(stats["missing_ids"]) + len(stats["wrong_type_ids"]),
                    }
                    self._json({"ok": True, "output": os.path.abspath(cfg["output"]),
                                "ids_path": os.path.abspath(cfg["ids_path"]),
                                "report": report})

                elif self.path == "/expand":
                    length = int(self.headers.get("Content-Length", 0))
                    payload = json.loads(self.rfile.read(length) or b"{}")
                    seeds = list(payload.get("ids", []))
                    mode = payload.get("mode", "")
                    adj = cfg["parents"] if mode == "ancestors" else cfg["children"]
                    rel = _closure(seeds, adj)
                    allids = sorted(set(seeds) | rel,
                                    key=lambda x: order.get(x, 1e9))
                    self._json({"ok": True, "ids": allids,
                                "added": len(rel - set(seeds))})

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
        description="Pick individuals in a browser; save the list or prune.")
    ap.add_argument("gedcom", help="GEDCOM file to list individuals from")
    ap.add_argument("--ids", default="ids.txt",
                    help="selection list to read/write (default ids.txt)")
    ap.add_argument("--output", default=None,
                    help="trimmed GEDCOM for Save & Prune "
                         "(default: <input>.trimmed.ged)")
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

    people = build_individuals(args.gedcom)
    parents, children = build_relationships(args.gedcom)
    cfg = {
        "people": people,
        "name_map": {p["id"]: p["name"] for p in people},
        "parents": parents,
        "children": children,
        "ids_path": args.ids,
        "gedcom": args.gedcom,
        "output": args.output,
        "prune_sources": not (args.keep_orphans or args.keep_orphan_sources),
        "prune_media": not args.keep_orphans,
    }

    httpd = HTTPServer(("127.0.0.1", args.port), make_handler(cfg))
    url = f"http://127.0.0.1:{args.port}/"
    print(f"Loaded {len(people)} individuals from {args.gedcom}")
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
