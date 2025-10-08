"""InfoTracker Column Lineage visualiser (no external libs, DOM+SVG).

This module reads column-level lineage edges and returns a single HTML file
that renders tables as green cards with column rows and draws SVG wires
between the left/right edges of the corresponding rows.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple, Optional

Edge = Dict[str, str]


# ---------------- I/O ----------------
def _load_edges(graph_path: Path) -> Sequence[Edge]:
    data = json.loads(graph_path.read_text(encoding="utf-8"))
    return data.get("edges", [])


def _load_schema_orders(dir_or_graph: Path) -> Dict[str, List[str]]:
    """Load column order per table from OpenLineage artifacts located next to column_graph.json.

    Returns mapping: "<namespace>.<schema.table>" (lowercase) -> [col1, col2, ...]
    """
    # Accept either the graph path (file) or its parent directory
    base_dir = dir_or_graph.parent if dir_or_graph.is_file() else dir_or_graph
    orders: Dict[str, List[str]] = {}
    try:
        for p in base_dir.glob("*.json"):
            # skip the graph file itself if present
            if p.name == "column_graph.json":
                continue
            try:
                j = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            outs = j.get("outputs") or []
            if not outs:
                continue
            out = outs[0] or {}
            ns = out.get("namespace")
            nm = out.get("name")
            if not (ns and nm):
                continue
            facets = out.get("facets") or {}
            schema = facets.get("schema") or {}
            fields = schema.get("fields") or []
            cols = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
            if cols:
                key = f"{ns}.{nm}".lower()
                orders[key] = cols
    except Exception:
        # fail-soft: no external ordering
        return {}
    return orders


# ---------------- Model ➜ Simple structures ----------------
def _parse_uri(uri: str) -> Tuple[str, str, str]:
    ns_tbl, col = uri.rsplit(".", 1)
    ns, tbl = ns_tbl.rsplit(".", 1)
    return ns, tbl, col


def _table_key(ns: str, tbl: str) -> str:
    return f"{ns}.{tbl}".lower()


def _build_elements(edges: Iterable[Edge], orders: Optional[Dict[str, List[str]]] = None) -> Tuple[List[Dict], List[Dict]]:
    """Build simple tables/edges lists for the HTML to render.

    tables: [{ id, label, full, columns: [str, ...] }]
    edges:  passthrough list of { from, to, transformation?, description? }
    """
    tables: Dict[str, Dict] = {}
    for e in edges:
        s = _parse_uri(e["from"])
        t = _parse_uri(e["to"])
        for ns, tbl, col in (s, t):
            key = _table_key(ns, tbl)
            tables.setdefault(
                key,
                {
                    "id": key,
                    "label": tbl,
                    "full": f"{ns}.{tbl}",
                    "namespace": ns,
                    "columns": [],  # keep insertion order
                },
            )
            if col not in tables[key]["columns"]:
                tables[key]["columns"].append(col)

    table_list: List[Dict] = []
    orders = orders or {}
    for key, t in tables.items():
        cols = list(t["columns"])  # already in encounter order
        # If we have OpenLineage schema order for this table, apply it
        order = orders.get(t["full"].lower())
        if order:
            pos = {c.lower(): i for i, c in enumerate(order)}
            cols.sort(key=lambda c: pos.get(c.lower(), 10**9))
        table_list.append({
            "id": key,
            "label": t["label"],
            "full": t["full"],
            "columns": cols,
        })

    return table_list, list(edges)


# ---------------- HTML template ----------------
HTML_TMPL = """<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\"/>
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>
<title>InfoTracker Lineage</title>
<style>
  :root{
    --bg:#f7f8fa; --card:#e6f0db; --card-target:#e9f1d1; --fx:#d9dde6;
    --header:#7fbf5f; --header-text:#fff; --border:#b8c5a6; --text:#1f2d1f;
    --row:#edf7e9; --row-alt:#e6f4e2; --row-border:#cbe4c0;
    --wire:#97a58a; --wire-strong:#6a7a5b;
    /* Selection highlight (accessible in light theme) */
    --sel-bg:#fde68a; /* amber-300 */
    --sel-outline:#111827; /* slate-900 */
    /* Sidebar background (light theme) */
    --sidebar-bg-start:#eef2f7; /* soft slate */
    --sidebar-bg-end:#e6ebf2;
  }
  html,body{height:100%; margin:0}
  body{display:flex; flex-direction:column; background:var(--bg); color:var(--text); font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, Arial}
  /* Modern toolbar styling */
  #toolbar{
    position:sticky; top:0; z-index:50;
    display:flex; align-items:center; gap:8px;
    padding:10px 12px;
    background: linear-gradient(180deg, rgba(255,255,255,0.70), rgba(255,255,255,0.55)) padding-box;
    -webkit-backdrop-filter: blur(8px) saturate(140%);
    backdrop-filter: blur(8px) saturate(140%);
    border-bottom:1px solid #e5e7eb;
    box-shadow: 0 2px 10px rgba(0,0,0,0.04);
  }
  #toolbar button{
    appearance:none; -webkit-appearance:none;
    padding:6px 12px; height:32px; line-height:20px;
    border:1px solid #cbd5e1; border-radius:8px; cursor:pointer;
    background: linear-gradient(180deg, #f8fafc, #eef2f7);
    color:#0f172a; font-weight:600; letter-spacing: .01em;
    box-shadow: 0 1px 0 rgba(255,255,255,0.8) inset, 0 1px 2px rgba(0,0,0,0.04);
    transition: background .15s ease, transform .05s ease, border-color .15s ease, box-shadow .15s ease;
  }
  #toolbar button:hover{ background: linear-gradient(180deg, #ffffff, #f1f5f9); }
  #toolbar button:active{ transform: translateY(0.5px); }
  #toolbar button:focus-visible{ outline:2px solid #60a5fa; outline-offset:2px; }
  /* make buttons feel like a group */
  #toolbar button + button{ margin-left:-1px; }
  #toolbar button:first-of-type{ border-top-right-radius:0; border-bottom-right-radius:0; }
  #toolbar button:nth-of-type(2){ border-radius:0; }
  #toolbar button:nth-of-type(3){ border-top-left-radius:0; border-bottom-left-radius:0; }
  /* search field with magnifier */
  #toolbar input{
    flex:1 1 360px; min-width:160px; height:34px;
    padding:6px 12px 6px 34px; border:1px solid #cbd5e1; border-radius:999px;
    background:
      url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="%236b7280" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>') 10px center / 16px 16px no-repeat,
      linear-gradient(180deg, #ffffff, #f8fafc);
    color:#111827;
    box-shadow: 0 1px 0 rgba(255,255,255,0.8) inset;
    transition: border-color .15s ease, box-shadow .15s ease;
  }
  /* Compact numeric input for depth */
  #toolbar input.compact{
    flex:0 0 auto; width:88px; min-width:88px; padding:6px 10px; border-radius:10px; background: linear-gradient(180deg, #ffffff, #f8fafc);
    background-image:none;
  }
  #toolbar input::placeholder{ color:#94a3b8 }
  #toolbar input:focus{ border-color:#60a5fa; box-shadow: 0 0 0 3px rgba(96,165,250,0.25); outline: none }
  /* Keep sidebar toggle separated from the 3-button group */
  #btnToggleSidebar{ margin-left:8px }
  /* Theme toggle switch */
  .theme-toggle{ display:flex; align-items:center; gap:8px; margin-left:8px; user-select:none }
  .theme-toggle input{ position:absolute; opacity:0; width:0; height:0 }
  .theme-toggle .switch{ width:46px; height:24px; border-radius:999px; background:#e5e7eb; border:1px solid #cbd5e1; position:relative; cursor:pointer; box-shadow: inset 0 1px 0 rgba(255,255,255,0.8) }
  .theme-toggle .switch::after{ content:""; position:absolute; top:2px; left:2px; width:20px; height:20px; border-radius:50%; background:#ffffff; box-shadow: 0 1px 2px rgba(0,0,0,0.15); transition:left .18s ease, background .18s ease }
  .theme-toggle input:checked + .switch{ background:#111827; border-color:#111827 }
  .theme-toggle input:checked + .switch::after{ left:24px; background:#0ea5e9 }
  
  /* Dark mode adjustments */
  @media (prefers-color-scheme: dark){
    :root{ --bg:#0b1020; --card:#13202b; --card-target:#1a2936; --fx:#273043; --header:#2c7d4d; --header-text:#e8f2e8; --border:#203042; --text:#e5eef5; --row:#132a1f; --row-alt:#0f241b; --row-border:#1f3a2e; --wire:#8da891; --wire-strong:#a2c79f; --sel-bg:#374151; /* slate-700 */ --sel-outline:#e5eef5; --sidebar-bg-start:rgba(11,16,32,0.70); --sidebar-bg-end:rgba(11,16,32,0.55); }
    #toolbar{ background: linear-gradient(180deg, rgba(11,16,32,0.65), rgba(11,16,32,0.55)); border-bottom-color:#1e293b; box-shadow: 0 2px 10px rgba(0,0,0,0.35); }
    #toolbar button{ background: linear-gradient(180deg, #0f172a, #0b1220); border-color:#243044; color:#e5eef5; box-shadow: 0 1px 0 rgba(255,255,255,0.04) inset, 0 1px 2px rgba(0,0,0,0.3); }
    #toolbar button:hover{ background: linear-gradient(180deg, #121a30, #0e1527); }
    #toolbar input{
      border-color:#243044; color:#e5eef5;
      background:
        url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="%2399a3b8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>') 10px center / 16px 16px no-repeat,
        linear-gradient(180deg, #101826, #0b1220);
      box-shadow: 0 1px 0 rgba(255,255,255,0.02) inset;
    }
    #toolbar input::placeholder{ color:#94a3b8 }
    #toolbar input:focus{ border-color:#60a5fa; box-shadow: 0 0 0 3px rgba(59,130,246,0.25); }
    /* Sidebar dark adjustments */
  #sidebar{ background: linear-gradient(180deg, var(--sidebar-bg-start), var(--sidebar-bg-end)); border-right-color:#1e293b; box-sizing:border-box }
    #sidebar .side-top{ background: linear-gradient(180deg, rgba(11,16,32,0.65), rgba(11,16,32,0.50)); border-bottom-color:#1e293b; box-shadow: 0 2px 8px rgba(0,0,0,0.35) }
    #sidebar .side-filter{ border-color:#243044; color:#e5eef5; background: linear-gradient(180deg, #101826, #0b1220); box-shadow: 0 1px 0 rgba(255,255,255,0.02) inset }
    #sidebar .tbl-item input:checked + span{ color:#e5eef5 }
  }
  /* Main split: left sidebar + right canvas */
  #content{display:flex; flex:1 1 auto; min-height:0}
  #sidebar{flex:0 0 auto; width:280px; min-width:160px; overflow-y:auto; overflow-x:hidden; border-right:1px solid #e5e7eb; padding:10px; background:linear-gradient(180deg, var(--sidebar-bg-start), var(--sidebar-bg-end)); box-sizing:border-box; transition: width .15s ease, padding .15s ease, border-color .15s ease }
  #sidebar.collapsed{ width:0; padding:0; border:none; overflow:hidden }
  #sidebar.collapsed *{ display:none }
  /* Draggable vertical resizer between sidebar and canvas */
  .side-resizer{ flex:0 0 auto; width:6px; cursor:col-resize; background:transparent; position:relative; z-index:30 }
  .side-resizer::after{ content:""; position:absolute; top:0; bottom:0; left:2px; width:2px; background:#e5e7eb; opacity:.9 }
  .side-resizer:hover::after{ background:#94a3b8 }
  body.resizing .side-resizer::after{ background:#64748b }
  body.resizing{ cursor: col-resize; user-select: none }
  .theme-dark .side-resizer::after{ background:#1f2937 }
  .theme-dark .side-resizer:hover::after{ background:#475569 }
  .theme-dark body.resizing .side-resizer::after{ background:#64748b }
  .side-resizer.hidden{ width:0 !important; cursor:default }
  .side-resizer.hidden::after{ display:none }
  #sidebar *{ box-sizing: border-box }
  #sidebar .side-top{position:sticky; top:0; z-index:5; padding:6px 2px 10px 2px; margin:-10px -10px 10px -10px; background: linear-gradient(180deg, rgba(255,255,255,0.82), rgba(255,255,255,0.66)); border-bottom:1px solid #e5e7eb; box-shadow: 0 2px 8px rgba(0,0,0,0.04)}
  #sidebar .side-header{padding:0 12px; font-weight:800; font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:#64748b; margin:4px 0 8px}
  #sidebar .side-actions{ display:flex; align-items:center; justify-content:flex-end; gap:8px; padding:0 12px 8px 12px }
  #sidebar .side-actions button{ appearance:none; -webkit-appearance:none; height:28px; padding:4px 10px; border:1px solid #cbd5e1; border-radius:8px; cursor:pointer; background: linear-gradient(180deg, #ffffff, #f8fafc); color:#0f172a; font-weight:600; letter-spacing:.01em }
  #sidebar .side-actions button:hover{ background: linear-gradient(180deg, #ffffff, #f1f5f9) }
  #sidebar .side-filter{display:block; width:calc(100% - 24px); max-width:calc(100% - 24px); margin:0 12px; height:34px; padding:6px 12px 6px 34px; border:1px solid #cbd5e1; border-radius:10px; background:
      url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="%236b7280" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>') 10px center / 16px 16px no-repeat,
      linear-gradient(180deg, #ffffff, #f8fafc);
    color:#0f172a; box-shadow: 0 1px 0 rgba(255,255,255,0.8) inset}
  #sidebar .side-filter::placeholder{ color:#94a3b8 }
  #sidebar .side-filter:focus{ outline:none; border-color:#60a5fa; box-shadow: 0 0 0 3px rgba(96,165,250,0.25) }
  #sidebar .tbl-item{display:flex; align-items:center; gap:10px; padding:8px 10px; border-radius:10px; cursor:pointer; margin:4px 8px; width:calc(100% - 16px)}
    #sidebar .tbl-item:hover{background: rgba(148,163,184,0.14)}
    #sidebar input[type="checkbox"]{width:16px; height:16px; accent-color: var(--header)}
  #sidebar .tbl-item input:checked + span{ font-weight:600; color:#0f172a }
  #sidebar .tbl-item .item-label{ flex:1 1 auto; min-width:0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis }
    #sidebar::-webkit-scrollbar{ width:10px }
    #sidebar::-webkit-scrollbar-thumb{ background: #cbd5e1; border-radius:10px }
    #sidebar::-webkit-scrollbar-track{ background: transparent }
  #viewport{position:relative; flex:1 1 auto; min-height:0; overflow:auto}
  #stage{position:relative; min-width:100%; min-height:100%; transform-origin: 0 0;}
  svg.wires{position:absolute; inset:0; pointer-events:none; width:100%; height:100%; z-index:20}
  /* Context menu */
  .ctx-menu{ position:fixed; z-index:1000; min-width:180px; background:#fff; color:#111827;
    border:1px solid #e5e7eb; border-radius:10px; box-shadow:0 8px 30px rgba(0,0,0,.12);
    padding:6px; display:none; }
  .theme-dark .ctx-menu{ background:#0b1020; color:#e5eef5; border-color:#1e293b; }
  .ctx-item{ padding:8px 10px; border-radius:8px; cursor:pointer; user-select:none; }
  .ctx-item:hover{ background:#f1f5f9 } .theme-dark .ctx-item:hover{ background:#121a30 }
  .ctx-sep{ height:1px; margin:6px 4px; background:#e5e7eb } .theme-dark .ctx-sep{ background:#1e293b }
  /* Isolation */
  .hidden{ display:none !important }
  .empty{position:absolute; left:20px; top:20px; color:#6b7280; font-size:14px}
  .empty{ top:80px }
  .table-node{position:absolute; width:240px; background:var(--card); border:1px solid var(--border); border-radius:10px; box-shadow:0 1px 2px rgba(0,0,0,.06)}
  .table-node{ cursor: grab; user-select: none; }
  .table-node.dragging{ box-shadow:0 6px 24px rgba(0,0,0,.18); cursor: grabbing; }
  .table-node header{position:relative; padding:8px 10px; font-weight:600; color:var(--header-text); background:var(--header); border-bottom:1px solid var(--border); border-radius:10px 10px 0 0; text-align:center}
  .table-node header .title{ display:inline-block; pointer-events:none }
  .table-node header .sel-btn{ position:absolute; right:8px; top:6px; height:24px; padding:2px 8px; border:1px solid rgba(255,255,255,.6); border-radius:6px; background:rgba(255,255,255,.15); color:#fff; font-weight:700; cursor:pointer }
  .table-node header .sel-btn:hover{ background:rgba(255,255,255,.25) }
  .table-node.selected{ box-shadow: 0 0 0 2px rgba(99,102,241,.35), 0 6px 18px rgba(0,0,0,.12) }
  .table-node ul{list-style:none; margin:0; padding:6px 10px 10px}
  .table-node.collapsed ul{ display:none }
  .table-node li{display:flex; align-items:center; justify-content:center; gap:8px; margin:4px 0; padding:6px 8px; background:var(--row); border:1px solid var(--row-border); border-radius:8px; white-space:nowrap; font-size:13px}
  .table-node li.alt{ background:var(--row-alt) }
  .table-node li.col-row{ cursor: pointer; }
  .table-node li.active{ outline:2px solid #6a7a5b }
  .table-node li.selected{ outline:2px solid var(--sel-outline); background: var(--sel-bg); color: var(--text) }
  .table-node li.col-row:hover{ border-color:#9bb1c9; box-shadow:0 1px 0 rgba(255,255,255,.7) inset, 0 1px 2px rgba(0,0,0,.05) }
  .table-node li.col-row:focus-visible{ outline:2px solid #60a5fa; outline-offset:2px }
  .table-node li .name{ user-select:none }
  .dim{ opacity: .22 }
  .port{display:inline-block; width:8px; height:8px; border-radius:50%; background:#6a7a5b; box-shadow:0 0 0 2px #fff inset}
  .port.right{ margin-left:8px }
  .port.left{ margin-right:8px }
  .table-node.target{ background:var(--card-target) }
  /* Search hits: visible regardless of dim; subtle but clear */
  .table-node.hit{ box-shadow: 0 0 0 2px rgba(99,102,241,.35), 0 6px 18px rgba(0,0,0,.12) }
  .table-node li.hit{ outline:2px dashed var(--sel-outline); background: var(--fx); position: relative }
  .table-node.hit, .table-node li.hit{ opacity: 1 !important; }
  svg .wire{fill:none; stroke:var(--wire-strong); stroke-width:2.4; stroke-linecap:round; stroke-linejoin:round}
  svg .wire.strong{stroke-width:3.2}
  svg .wire.neighbor{ stroke-dasharray:6 6; opacity:.9 }
  svg defs marker#arrow{ overflow:visible }
  /* Neighbor (ghost) tables rendered for context when selecting a table */
  .table-node.neighbor{ opacity:.75; border-style:dashed; border-color:#94a3b8; background:transparent }
  .table-node.neighbor header{ background:#9aa6b2; color:#0b1020 }
</style>
</head>
<body>
<div id="toolbar">
  <button id="btnFit" title="Fit to content">Fit</button>
  <button id="btnZoomOut" title="Zoom out">−</button>
  <button id="btnZoomIn" title="Zoom in">+</button>
  <button id="btnToggleSidebar" title="Hide/show sidebar">Sidebar</button>
  <input id="depthInput" class="compact" type="number" min="0" step="1" title="Neighbor depth (0 = unlimited)" />
  <button id="btnToggleCollapse" title="Collapse/expand columns">Collapse</button>
  <input id="search" type="text" placeholder="Search table/column… (Enter to jump)" />
  <label class="theme-toggle" title="Toggle dark mode">
    <input id="themeToggle" type="checkbox" aria-label="Dark mode" />
    <span class="switch"></span>
  </label>
</div>
<div id="content">
  <aside id="sidebar" aria-label="Tables">
    <div class="side-top">
      <div class="side-header">Objects</div>
      <div class="side-actions">
        <button id="btnClearAll" title="Uncheck all">Clear</button>
        <button id="btnSelectAll" title="Select all objects">Select All</button>
      </div>
      <input id="sideFilter" class="side-filter" type="text" placeholder="Filter objects or columns…" />
    </div>
    <div id="tableList"></div>
  </aside>
  <div id="sidebarResizer" class="side-resizer" role="separator" aria-orientation="vertical" aria-label="Resize sidebar" tabindex="0"></div>
  <div id="viewport">
    <div id="stage"></div>
    <svg class="wires" id="wires" aria-hidden="true">
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3.5" orient="auto">
          <polygon points="0 0, 7 3.5, 0 7" fill="var(--wire-strong)"/>
        </marker>
        <!-- colorized arrow markers will be injected below (arrow-0..N) -->
      </defs>
    </svg>
  </div>
</div>
<!-- floating context menu for attribute rows -->
<div id="ctxMenu" class="ctx-menu" role="menu" aria-hidden="true"></div>
<script>
// ---- Theme handling ----
const THEME_KEY = 'infotracker.theme';
function systemTheme(){
  try{ return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'; }catch(_){ return 'light'; }
}
function applyTheme(mode){
  const root = document.documentElement;
  root.classList.remove('theme-dark','theme-light');
  root.classList.add(mode === 'dark' ? 'theme-dark' : 'theme-light');
}
function initTheme(){
  const saved = localStorage.getItem(THEME_KEY);
  const mode = (saved === 'dark' || saved === 'light') ? saved : systemTheme();
  applyTheme(mode);
  const t = document.getElementById('themeToggle');
  if (t) t.checked = (mode === 'dark');
}
function toggleTheme(toDark){
  const mode = toDark ? 'dark' : 'light';
  localStorage.setItem(THEME_KEY, mode);
  applyTheme(mode);
}
initTheme();
const EDGES = __EDGES__;
const __ALL_TABLES_RAW__ = __NODES__;
// Fallback: if nodes are missing, derive tables from edges
function __deriveTablesFromEdges(){
  const m = new Map(); // id -> {id,label,full,columns:Set}
  (EDGES||[]).forEach(e=>{
    [e.from, e.to].forEach(u=>{
      if (!u) return;
      const p = parseUri(u);
      const id = p.tableId;
      let t = m.get(id);
      if (!t){
        t = { id, label: p.tbl, full: p.ns + '.' + p.tbl, columns: new Set() };
        m.set(id, t);
      }
      t.columns.add(p.col);
    });
  });
  // Keep insertion order from edges; don't sort alphabetically
  return Array.from(m.values()).map(t=>({ id: t.id, label: t.label, full: t.full, columns: Array.from(t.columns) }));
}
const ALL_TABLES = (Array.isArray(__ALL_TABLES_RAW__) && __ALL_TABLES_RAW__.length) ? __ALL_TABLES_RAW__ : __deriveTablesFromEdges();
let TABLES = []; // visible tables: selected + neighbors
const CONFIG = { focus: __FOCUS__, depth: __DEPTH__, direction: __DIRECTION__ };
const SIDEBAR_KEY = 'infotracker.sidebar';
const SIDEBAR_W_KEY = 'infotracker.sidebar.width';
const COLLAPSE_KEY = 'infotracker.collapse';
let COLLAPSE = false;

// Helpers
const ROW_H = 30, GUTTER_Y = 16, GUTTER_X = 260, LEFT = 60, TOP = 60;
// Global scale used by pan/zoom and wire projection; must be defined before first draw
let SCALE = 1;
let FIRST_FIT_DONE = false;
let VISIBLE_IDS = new Set(); // explicitly selected via checkboxes
let NEIGHBOR_IDS = new Set(); // derived: immediate neighbors of selected
let FILTER_TEXT = '';

// Lineage highlight globals (declared early to avoid TDZ on first draw)
let COL_OUT = null; // Map colKey -> Array<edge>
let COL_IN = null;  // Map colKey -> Array<edge>
let ROW_BY_COL = new Map(); // colKey -> <li>
let PATH_BY_EDGE = new Map(); // edgeKey -> <path>
let SELECTED_COL = null;
// Isolation mode (context menu)
let ISOLATE = false;      // whether isolate-view is on
let ISOLATE_DIR = 'both'; // 'up' | 'down' | 'both'
let ISOLATE_SRC = null;   // source column key
// Search hit globals
let URI_BY_COL = null; // Map colKey -> example URI (from edges)

// Distinct, accessible palette (WCAG-friendly-ish) for edge coloring
const PALETTE = [
  '#2f855a', // green
  '#1d4ed8', // blue
  '#d97706', // amber
  '#b91c1c', // red
  '#7c3aed', // purple
  '#0d9488', // teal
  '#be123c', // rose
  '#065f46', // green-dark
  '#2563eb', // blue-bright
  '#ea580c'  // orange
];

// Build color assignment per source (e.from) so multiple outgoing wires differ in color
let EDGE_COLOR_IDX = null; // Map of edgeKey -> palette index
let OUT_DEG = null; // Map of source columnKey (tableId.col) -> out degree
function buildEdgeColors(){
  const byFromCol = new Map(); // columnKey -> edges[]
  EDGES.forEach(e=>{
    const s = parseUri(e.from);
    const key = (s.tableId + '.' + s.col).toLowerCase();
    const arr = byFromCol.get(key) || [];
    arr.push(e); byFromCol.set(key, arr);
  });
  const m = new Map();
  const outdeg = new Map();
  // deterministic: sort outgoing edges by to+transformation
  byFromCol.forEach((arr, fromColKey)=>{
    outdeg.set(fromColKey, arr.length);
    arr.sort((a,b)=>{
      const ka = (a.to||'') + '|' + (a.transformation||'');
      const kb = (b.to||'') + '|' + (b.transformation||'');
      return ka.localeCompare(kb);
    });
    arr.forEach((e, i)=>{
      const key = edgeKey(e);
      m.set(key, i % PALETTE.length);
    });
  });
  EDGE_COLOR_IDX = m;
  OUT_DEG = outdeg;
  ensureColorMarkers();
}

function edgeKey(e){
  return (e.from||'') + '->' + (e.to||'') + ':' + (e.transformation||'');
}

function ensureColorMarkers(){
  const svg = document.getElementById('wires');
  if (!svg) return;
  // Ensure <defs> exists with base marker
  let defs = svg.querySelector('defs');
  if (!defs){
    defs = document.createElementNS('http://www.w3.org/2000/svg','defs');
    svg.insertBefore(defs, svg.firstChild);
  }
  if (!defs.querySelector('#arrow')){
    const m = document.createElementNS('http://www.w3.org/2000/svg','marker');
    m.setAttribute('id','arrow');
    m.setAttribute('markerWidth','8');
    m.setAttribute('markerHeight','8');
    m.setAttribute('refX','6');
    m.setAttribute('refY','3.5');
    m.setAttribute('orient','auto');
    const poly = document.createElementNS('http://www.w3.org/2000/svg','polygon');
    poly.setAttribute('points','0 0, 7 3.5, 0 7');
    poly.setAttribute('fill','var(--wire-strong)');
    m.appendChild(poly);
    defs.appendChild(m);
  }
  // create markers for all palette indices if missing
  PALETTE.forEach((col, idx)=>{
    const id = 'arrow-' + idx;
    if (defs.querySelector('#'+id)) return;
    const m = document.createElementNS('http://www.w3.org/2000/svg','marker');
    m.setAttribute('id', id);
    m.setAttribute('markerWidth','8');
    m.setAttribute('markerHeight','8');
    m.setAttribute('refX','6');
    m.setAttribute('refY','3.5');
    m.setAttribute('orient','auto');
    const poly = document.createElementNS('http://www.w3.org/2000/svg','polygon');
    poly.setAttribute('points','0 0, 7 3.5, 0 7');
    poly.setAttribute('fill', col);
    m.appendChild(poly);
    defs.appendChild(m);
  });
}

// Robust rsplit for "ns.tbl.col" (ns may contain dots)
function parseUri(u){
  const p1 = u.lastIndexOf('.');
  const col = u.slice(p1 + 1);
  const pre = u.slice(0, p1);
  const p0 = pre.lastIndexOf('.');
  const tbl = pre.slice(p0 + 1);
  const ns = pre.slice(0, p0);
  return { ns, tbl, col, tableId: (ns + '.' + tbl).toLowerCase(), colId: (ns + '.' + tbl + '.' + col).toLowerCase() };
}

// Build table graph by table ids
function buildGraph(){
  const ids = new Set(TABLES.map(t=>t.id));
  const adj = new Map([...ids].map(id=>[id,new Set()]));
  const indeg = new Map([...ids].map(id=>[id,0]));
  const pred = new Map([...ids].map(id=>[id,new Set()]));
  EDGES.forEach(e=>{
    const s=parseUri(e.from), t=parseUri(e.to);
    // Only consider edges between currently visible tables
    if (s.tableId!==t.tableId && ids.has(s.tableId) && ids.has(t.tableId)){
      const sAdj = adj.get(s.tableId);
      if (sAdj && !sAdj.has(t.tableId)){
        sAdj.add(t.tableId);
        indeg.set(t.tableId, (indeg.get(t.tableId) || 0) + 1);
        const pset = pred.get(t.tableId);
        if (pset) pset.add(s.tableId);
      }
    }
  });
  return {adj, indeg, pred};
}

function ranksFromGraph(graph){
  const {adj, indeg} = graph;
  const r = new Map();
  const q = [];
  indeg.forEach((v,k)=>{ if(v===0) q.push(k); });
  if (!q.length && indeg.size) q.push([...indeg.keys()].sort()[0]);
  while(q.length){
    const u=q.shift();
    const ru = r.get(u)||0; r.set(u,ru);
    adj.get(u).forEach(v=>{ const rv=Math.max(r.get(v)||0, ru+1); r.set(v,rv); indeg.set(v, indeg.get(v)-1); if(indeg.get(v)===0) q.push(v); });
  }
  [...indeg.keys()].forEach(k=>{ if(!r.has(k)) r.set(k,0); });
  return r;
}

function layoutTables(){
  const stage = document.getElementById('stage');
  // Clear only stage content (wires SVG stays a sibling under #viewport)
  stage.innerHTML = '';
  
  if (!TABLES || !TABLES.length){
    const info = document.createElement('div'); info.className='empty'; info.textContent = 'No tables selected. Use the left panel to choose tables.';
    stage.appendChild(info);
    // also clear wires
    const svg = document.getElementById('wires');
    while(svg.lastChild && svg.lastChild.tagName !== 'defs') svg.removeChild(svg.lastChild);
    return;
  }
  // compute edge colors once per layout
  buildEdgeColors();
  const graph = buildGraph();
  const r = ranksFromGraph(graph);
  const layers = new Map(); r.forEach((rv,id)=>{ if(!layers.has(rv)) layers.set(rv,[]); layers.get(rv).push(id); });
  // crossing minimization: barycentric forward/backward passes
  orderLayers(layers, graph);

  // Build DOM cards
  const cardById = new Map();
  TABLES.forEach(t=>{
    const art = document.createElement('article'); art.className='table-node'; art.id = `tbl-${t.id}`;
    if (NEIGHBOR_IDS && NEIGHBOR_IDS.has(t.id)) art.classList.add('neighbor');
    if (COLLAPSE) art.classList.add('collapsed');
    // Attach searchable metadata
    art.setAttribute('data-id', (t.id||'').toLowerCase());
    art.setAttribute('data-full', (t.full||'').toLowerCase());
    art.setAttribute('data-label', (t.label||'').toLowerCase());
    const h = document.createElement('header'); h.title = t.full || t.label;
    const title = document.createElement('span'); title.className='title'; title.textContent = t.label; h.appendChild(title);
    const isSel = VISIBLE_IDS && VISIBLE_IDS.has && VISIBLE_IDS.has(t.id);
    if (isSel) art.classList.add('selected');
    const btn = document.createElement('button'); btn.className='sel-btn'; btn.type='button'; btn.textContent = isSel ? '−' : '+'; btn.title = isSel ? 'Unselect' : 'Select'; btn.dataset.tid = t.id;
    btn.addEventListener('click', (ev)=>{ ev.stopPropagation(); ev.preventDefault(); toggleTableSelection(ev.currentTarget.dataset.tid); });
    h.appendChild(btn);
    // dblclick header toggles selection as well
    h.addEventListener('dblclick', (ev)=>{ ev.stopPropagation(); toggleTableSelection(t.id); });
    art.appendChild(h);
    const ul = document.createElement('ul');
    t.columns.forEach((c, i)=>{
      const li = document.createElement('li'); if(i%2) li.classList.add('alt');
      // left/right ports for precise anchoring
      const left = document.createElement('span'); left.className='port left';
      const txt = document.createElement('span'); txt.className='name'; txt.textContent = c;
      const right = document.createElement('span'); right.className='port right';
      const key = `${t.id}.${c}`.toLowerCase();
      left.setAttribute('data-key', key); left.setAttribute('data-side','L');
      right.setAttribute('data-key', key); right.setAttribute('data-side','R');
      // make whole row clickable and focusable immediately
      li.classList.add('col-row');
      li.setAttribute('data-key', key);
      li.setAttribute('tabindex','0');
      li.setAttribute('role','button');
      li.addEventListener('click', onRowClick);
      li.addEventListener('keydown', (ev)=>{ if (ev.key==='Enter' || ev.key===' '){ ev.preventDefault(); onRowClick(ev); } });
      li.appendChild(left); li.appendChild(txt); li.appendChild(right);
      ul.appendChild(li);
    });
    art.appendChild(ul);
    stage.appendChild(art);
    cardById.set(t.id, art);
    makeDraggable(art);
  });
  // Do not move the wires SVG; it remains a sibling of #stage
  // Rows exist now -> (re)build column graph and mark rows clickable (only when expanded)
  if (!COLLAPSE){ try { buildColGraph(); } catch(_) {} }
  // Sizes
  const maxWidth = Math.max(240, ...[...cardById.values()].map(el=>{
    const w = Math.max(el.querySelector('header').offsetWidth, ...Array.from(el.querySelectorAll('li span:nth-child(2)')).map(s=>s.offsetWidth+60));
    return Math.min(420, w+24);
  }));

  // Precompute approximate content height (tallest column) to support vertical centering
  const rankHeights = new Map();
  layers.forEach((ids, rk)=>{
    const hs = ids.map(id=> cardById.get(id).offsetHeight);
    const n = hs.length;
    const sumH = hs.reduce((a,b)=>a+b,0);
    const colH = n ? sumH + (n-1)*GUTTER_Y : 0;
    rankHeights.set(rk, colH);
  });
  const maxColH = Math.max(0, ...rankHeights.values());
  const viewportEl = document.getElementById('viewport');
  const viewW = viewportEl.clientWidth / SCALE;
  const viewH = viewportEl.clientHeight / SCALE;
  const columns = Math.max(1, Math.max(...layers.keys()) + 1);
  const contentW = columns * maxWidth + (columns - 1) * GUTTER_X;
  const baseLeft = Math.max(LEFT, Math.floor((viewW - contentW) / 2));
  const baseTop = Math.max(TOP, Math.floor((viewH - maxColH) / 2));

  const maxRank = Math.max(...layers.keys());
  let maxRight = 0, maxBottom = 0;
  const centerMap = new Map(); // tableId -> centerY

  for(let rk=0; rk<=maxRank; rk++){
    const x = baseLeft + rk*(maxWidth + GUTTER_X);
    const ids = layers.get(rk)||[];
    // build items with preferred center from predecessors
    const items = ids.map(id=>{
      const card = cardById.get(id);
      const preds = (graph.pred.get(id) || new Set());
      const centers = [];
      preds.forEach(p=>{ const c = centerMap.get(p); if (c!=null) centers.push(c); });
      const pref = centers.length ? (centers.reduce((a,b)=>a+b,0)/centers.length) : null;
      return { id, card, pref, h: card.offsetHeight };
    });
    // sort by preferred center (so related tables land on similar Y)
    items.sort((a,b)=>{
      const aa = a.pref==null ? Infinity : a.pref;
      const bb = b.pref==null ? Infinity : b.pref;
      if (aa===bb) return a.id.localeCompare(b.id);
      return aa-bb;
    });

  let currentTop = baseTop; // running top, ensures non-overlap and vertical centering
    items.forEach(it=>{
      const centerDesired = it.pref!=null ? it.pref : (currentTop + it.h/2);
      const center = Math.max(centerDesired, currentTop + it.h/2);
      const y = Math.round(center - it.h/2);
      it.card.style.width = `${maxWidth}px`;
      it.card.style.left = `${x}px`;
      it.card.style.top = `${y}px`;
      centerMap.set(it.id, center);
      const rightX = x + it.card.offsetWidth;
      const bottomY = y + it.card.offsetHeight;
      if (rightX > maxRight) maxRight = rightX;
      if (bottomY > maxBottom) maxBottom = bottomY;
      currentTop = y + it.h + GUTTER_Y;
    });
  }

  // Expand stage and SVG to content bounds
  const stageRectW = Math.ceil(maxRight + LEFT);
  const stageRectH = Math.ceil(maxBottom + TOP);
  stage.style.width = stageRectW + 'px';
  stage.style.height = stageRectH + 'px';
  // Sync wires box and scale to stage dimensions and current SCALE
  updateWiresSize();

  drawEdges();
  requestAnimationFrame(drawEdges);

  // setup click handler for lineage highlight (event delegation)
  stage.onclick = onStageClick;
  stage.onkeydown = onStageKeyDown;

  // Reset selection state when the visible set changes
  SELECTED_COL = null;

  // auto-fit on first successful layout so users see content immediately
  if (!FIRST_FIT_DONE && TABLES && TABLES.length){
    FIRST_FIT_DONE = true;
    setTimeout(()=>{ try{ fitToContent(); }catch(_){} }, 0);
  }
}

// Keep SVG wires in sync with stage size and zoom SCALE
function updateWiresSize(){
  const svg = document.getElementById('wires');
  const stage = document.getElementById('stage');
  if (!svg || !stage) return;
  const w = Math.ceil(stage.offsetWidth);
  const h = Math.ceil(stage.offsetHeight);
  // Coordinate space stays unscaled (viewBox in stage units)
  svg.setAttribute('width', String(w));
  svg.setAttribute('height', String(h));
  svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
  // Visual size follows current SCALE so paths (computed in stage units)
  // align with CSS-transformed stage
  svg.style.width = (w * SCALE) + 'px';
  svg.style.height = (h * SCALE) + 'px';
}

function centerOf(el){
  const r = el.getBoundingClientRect();
  const s = document.getElementById('stage').getBoundingClientRect();
  const x = (r.left - s.left + r.width/2) / SCALE;
  const y = (r.top - s.top + r.height/2) / SCALE;
  return { x, y };
}

// Anchor on left/right side of a card (article.table-node)
function anchorOfCard(card, side){
  const r = card.getBoundingClientRect();
  const s = document.getElementById('stage').getBoundingClientRect();
  const y = (r.top - s.top + r.height/2) / SCALE;
  const x = side === 'R' ? (r.right - s.left) / SCALE : (r.left - s.left) / SCALE;
  return { x, y };
}

function drawEdges(){
  const svg = document.getElementById('wires');
  // clear old
  while(svg.lastChild && svg.lastChild.tagName !== 'defs') svg.removeChild(svg.lastChild);
  // Recreate markers if needed
  ensureColorMarkers();

  PATH_BY_EDGE.clear();
  // Collapsed mode: draw aggregated table->table edges
  if (COLLAPSE){
    drawEdgesCollapsed();
    return;
  }
  const selectedIds = new Set(VISIBLE_IDS);
  const neighborIds = new Set(NEIGHBOR_IDS);
  const visibleIds = new Set(TABLES.map(t=>t.id));
  EDGES.forEach(e=>{
    const s = parseUri(e.from), t = parseUri(e.to);
    if (!visibleIds.has(s.tableId) || !visibleIds.has(t.tableId)) return;
    const sKey = (s.tableId + '.' + s.col).toLowerCase();
    const tKey = (t.tableId + '.' + t.col).toLowerCase();
    const sp = document.querySelector(`.port[data-key="${sKey}"][data-side="R"]`);
    const tp = document.querySelector(`.port[data-key="${tKey}"][data-side="L"]`);
    if(!sp || !tp) return;
    const a = centerOf(sp); const b = centerOf(tp);
    const dx = Math.max(120, Math.abs(b.x - a.x)/2);
    const d = `M ${a.x} ${a.y} C ${a.x+dx} ${a.y}, ${b.x-dx} ${b.y}, ${b.x} ${b.y}`;
    const p = document.createElementNS('http://www.w3.org/2000/svg','path');
    p.setAttribute('d', d);
    p.setAttribute('class','wire'+(e.transformation && e.transformation!=='IDENTITY' ? ' strong':'') );
    // Style neighbor edges (connecting any neighbor table) as dashed
    const isNeighborEdge = (neighborIds.has(s.tableId) || neighborIds.has(t.tableId)) && !(selectedIds.has(s.tableId) && selectedIds.has(t.tableId));
    if (isNeighborEdge) p.classList.add('neighbor');
    const ek = edgeKey(e);
    p.setAttribute('data-edge-key', ek);
    // colorize by source column only if that column has multiple outgoing edges
    const sColKey = (s.tableId + '.' + s.col).toLowerCase();
    const deg = OUT_DEG && OUT_DEG.get(sColKey);
    if (deg && deg > 1){
      const idx = (EDGE_COLOR_IDX && EDGE_COLOR_IDX.get(edgeKey(e))) ?? 0;
      const col = PALETTE[idx % PALETTE.length];
      p.setAttribute('stroke', col);
      p.setAttribute('marker-end', `url(#arrow-${idx % PALETTE.length})`);
    } else {
      p.setAttribute('marker-end','url(#arrow)');
    }
    svg.appendChild(p);
    PATH_BY_EDGE.set(ek, p);
  });
  // Reapply highlight if a column is selected (scroll/resize triggers redraw)
  if (SELECTED_COL){
    try { highlightLineage(SELECTED_COL, ISOLATE_DIR || 'both', ISOLATE || false); } catch(_) {}
  }
}

function drawEdgesCollapsed(){
  const svg = document.getElementById('wires');
  const visibleIds = new Set(TABLES.map(t=>t.id));
  const neighborIds = new Set(NEIGHBOR_IDS);
  const selectedIds = new Set(VISIBLE_IDS);
  const pairs = new Set();
  EDGES.forEach(e=>{
    const s = parseUri(e.from), t = parseUri(e.to);
    if (s.tableId === t.tableId) return;
    if (!visibleIds.has(s.tableId) || !visibleIds.has(t.tableId)) return;
    pairs.add(s.tableId + '|' + t.tableId);
  });
  pairs.forEach(key=>{
    const [sid, tid] = key.split('|');
    const scard = document.getElementById('tbl-' + sid);
    const tcard = document.getElementById('tbl-' + tid);
    if (!scard || !tcard) return;
    const a = anchorOfCard(scard, 'R');
    const b = anchorOfCard(tcard, 'L');
    const dx = Math.max(120, Math.abs(b.x - a.x)/2);
    const d = `M ${a.x} ${a.y} C ${a.x+dx} ${a.y}, ${b.x-dx} ${b.y}, ${b.x} ${b.y}`;
    const p = document.createElementNS('http://www.w3.org/2000/svg','path');
    p.setAttribute('d', d);
    p.setAttribute('class','wire');
    const isNeighborEdge = (neighborIds.has(sid) || neighborIds.has(tid)) && !(selectedIds.has(sid) && selectedIds.has(tid));
    if (isNeighborEdge) p.classList.add('neighbor');
    p.setAttribute('marker-end','url(#arrow)');
    svg.appendChild(p);
  });
}

// Compute render sets: selected + immediate neighbors for context
function computeRenderSets(){
  const base = new Set(VISIBLE_IDS);
  const neighbors = new Set();
  if (base.size){
    // Build table-level adjacency once per call
    const out = new Map(); // tableId -> Set(neighbors)
    const inn = new Map();
    EDGES.forEach(e=>{
      const s = parseUri(e.from), t = parseUri(e.to);
      if (s.tableId === t.tableId) return;
      if (!out.has(s.tableId)) out.set(s.tableId, new Set());
      if (!inn.has(t.tableId)) inn.set(t.tableId, new Set());
      out.get(s.tableId).add(t.tableId);
      inn.get(t.tableId).add(s.tableId);
    });
    const dir = (CONFIG && CONFIG.direction) ? String(CONFIG.direction).toLowerCase() : 'both';
    const maxDepthRaw = (CONFIG && typeof CONFIG.depth !== 'undefined') ? parseInt(CONFIG.depth, 10) : 1;
    const maxDepth = isNaN(maxDepthRaw) ? 1 : maxDepthRaw; // 0 => unlimited
    let depth = 0;
    let frontier = new Set(base);
    const seen = new Set(base);
    while (frontier.size && (maxDepth === 0 || depth < maxDepth)){
      const next = new Set();
      frontier.forEach(u=>{
        if (dir === 'down' || dir === 'both'){
          const ns = out.get(u) || new Set();
          ns.forEach(v=>{ if (!seen.has(v)){ seen.add(v); neighbors.add(v); next.add(v); } });
        }
        if (dir === 'up' || dir === 'both'){
          const ps = inn.get(u) || new Set();
          ps.forEach(v=>{ if (!seen.has(v)){ seen.add(v); neighbors.add(v); next.add(v); } });
        }
      });
      frontier = next; depth++;
    }
  }
  NEIGHBOR_IDS = neighbors;
  const renderIds = new Set([...base, ...neighbors]);
  TABLES = ALL_TABLES.filter(x=> renderIds.has(x.id));
}

// Toggle table selection from canvas or programmatically; updates sidebar + layout
function toggleTableSelection(tableId){
  if (!tableId) return;
  if (VISIBLE_IDS.has(tableId)) VISIBLE_IDS.delete(tableId); else VISIBLE_IDS.add(tableId);
  computeRenderSets();
  layoutTables();
  buildSidebar();
}

// Build sidebar with checkboxes (all unchecked by default)
function buildSidebar(){
  const list = document.getElementById('tableList');
  if (!list) return;
  list.innerHTML = '';
  const items = [...ALL_TABLES].sort((a,b)=>{
    const la = (a.label||a.full||'').toLowerCase();
    const lb = (b.label||b.full||'').toLowerCase();
    if (la === lb) return (a.id||'').localeCompare(b.id||'');
    return la.localeCompare(lb);
  }).filter(t=>{
    if (!FILTER_TEXT) return true;
    const q = (FILTER_TEXT||'').toLowerCase();
    const label = (t.label||'').toLowerCase();
    const full = (t.full||'').toLowerCase();
    const idv  = (t.id||'').toLowerCase();
    // Column match: use last segment after dot to allow inputs like schema.table.column
    const colQuery = q.includes('.') ? q.split('.').pop() : q;
    const cols = Array.isArray(t.columns) ? t.columns : [];
    const hasCol = cols.some(c=> (c||'').toLowerCase().includes(colQuery));
    return label.includes(q) || full.includes(q) || idv.includes(q) || hasCol;
  });
  if (!items.length){
    const empty = document.createElement('div');
    empty.style.cssText = 'color:#64748b; font-size:12px; padding:8px 12px;';
    empty.textContent = FILTER_TEXT ? 'No objects match the filter.' : 'No objects available.';
    list.appendChild(empty);
    return;
  }
  items.forEach(t=>{
    const id = 'chk-' + t.id;
    const row = document.createElement('label');
    row.className = 'tbl-item';
    const cb = document.createElement('input'); cb.type='checkbox'; cb.id=id; cb.dataset.tid = t.id;
    cb.checked = VISIBLE_IDS.has(t.id);
    cb.addEventListener('change', (e)=>{
      const tid = e.currentTarget.dataset.tid;
      if (e.currentTarget.checked){ VISIBLE_IDS.add(tid); }
      else { VISIBLE_IDS.delete(tid); }
      computeRenderSets();
      layoutTables();
      // Auto-fit once content appears for the first time
      if (TABLES.length && !FIRST_FIT_DONE){ try{ fitToContent(); }catch(_){} }
      else { drawEdges(); }
    });
  const name = document.createElement('span'); name.className = 'item-label'; name.textContent = t.label || t.full || t.id;
    name.title = t.full || t.id;
    row.appendChild(cb); row.appendChild(name);
    list.appendChild(row);
  });
}

// Initialize sidebar collapsed state before first layout
try{
  const savedSide = localStorage.getItem(SIDEBAR_KEY);
  const collapsed = (savedSide === 'collapsed');
  const sideEl = document.getElementById('sidebar');
  if (collapsed && sideEl){ sideEl.classList.add('collapsed'); }
  // Ensure resizer mirrors collapsed state on load
  try{
    const resEl = document.getElementById('sidebarResizer');
    if (resEl){ resEl.classList.toggle('hidden', collapsed); }
  }catch(_){ }
  // Restore saved width
  const savedW = parseInt(localStorage.getItem(SIDEBAR_W_KEY)||'', 10);
  if (!isNaN(savedW) && sideEl && savedW >= 160){ sideEl.style.width = savedW + 'px'; }
  // Load saved collapse-columns preference
  const savedCollapse = localStorage.getItem(COLLAPSE_KEY);
  if (savedCollapse === '1' || savedCollapse === 'true') COLLAPSE = true;
}catch(_){}

buildSidebar();
computeRenderSets();
layoutTables();
window.addEventListener('resize', ()=>{ layoutTables(); });
document.getElementById('viewport').addEventListener('scroll', ()=>{ drawEdges(); });
// Live filter behavior
const sideFilter = document.getElementById('sideFilter');
if (sideFilter){
  sideFilter.addEventListener('input', (e)=>{
    FILTER_TEXT = (e.currentTarget.value||'').trim();
    buildSidebar();
  });
}

// Clear-all selection in sidebar
const btnClearAll = document.getElementById('btnClearAll');
if (btnClearAll){
  btnClearAll.addEventListener('click', ()=>{
    try{ clearSelection(); }catch(_){ }
    VISIBLE_IDS.clear();
    computeRenderSets();
    layoutTables();
    buildSidebar();
  });
}

// Select-all objects in sidebar
const btnSelectAll = document.getElementById('btnSelectAll');
if (btnSelectAll){
  btnSelectAll.addEventListener('click', ()=>{
    try{ clearSelection(); }catch(_){ }
    // Keep the same Set instance to avoid breaking references
    VISIBLE_IDS.clear();
    (ALL_TABLES || []).forEach(t=> VISIBLE_IDS.add(t.id));
    computeRenderSets();
    layoutTables();
    buildSidebar();
  });
}

// Sidebar resizer: drag to adjust width (persists), keyboard arrows supported
(function(){
  const res = document.getElementById('sidebarResizer');
  const side = document.getElementById('sidebar');
  if (!res || !side) return;
  let resizing = false, startX = 0, startW = 0;
  const MIN_W = 160;
  function maxWidth(){
    try{ return Math.max(240, Math.floor(window.innerWidth * 0.7)); }catch(_){ return 800; }
  }
  function applyWidth(w, persist){
    const clamped = Math.max(MIN_W, Math.min(maxWidth(), Math.round(w)));
    side.style.width = clamped + 'px';
    if (persist){ try{ localStorage.setItem(SIDEBAR_W_KEY, String(clamped)); }catch(_){ }
    }
    // Nodes don't move; just redraw wires to keep alignment during resize
    if (!window.__rafSide){
      window.__rafSide = true;
      requestAnimationFrame(()=>{ window.__rafSide = false; drawEdges(); });
    }
  }
  res.addEventListener('mousedown', (e)=>{
    if (side.classList.contains('collapsed')) return;
    resizing = true; startX = e.clientX; startW = side.offsetWidth;
    document.body.classList.add('resizing');
    e.preventDefault();
  });
  window.addEventListener('mousemove', (e)=>{
    if (!resizing) return;
    const dx = e.clientX - startX;
    applyWidth(startW + dx, false);
  });
  window.addEventListener('mouseup', ()=>{
    if (!resizing) return;
    resizing = false; document.body.classList.remove('resizing');
    const w = parseInt(side.style.width||'0', 10); if (w) applyWidth(w, true);
  });
  res.addEventListener('keydown', (e)=>{
    if (side.classList.contains('collapsed')) return;
    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight'){
      const cur = side.offsetWidth || MIN_W;
      const step = (e.shiftKey ? 40 : 10) * (e.key === 'ArrowLeft' ? -1 : 1);
      applyWidth(cur + step, true);
      e.preventDefault();
    } else if (e.key === 'Enter'){
      applyWidth(280, true);
      e.preventDefault();
    }
  });
  res.addEventListener('dblclick', ()=> applyWidth(280, true));
})();

// Theme toggle binding
const themeToggle = document.getElementById('themeToggle');
if (themeToggle){ themeToggle.addEventListener('change', (e)=>{ toggleTheme(!!e.currentTarget.checked); }); }
// Sync collapse button label on load
try{ updateCollapseButton(); }catch(_){ }

// Depth input wiring
const depthInput = document.getElementById('depthInput');
if (depthInput){
  const d = (CONFIG && typeof CONFIG.depth !== 'undefined') ? parseInt(CONFIG.depth, 10) : 1;
  depthInput.value = isNaN(d) ? 1 : d;
  depthInput.addEventListener('change', (e)=>{
    const v = parseInt(e.currentTarget.value, 10);
    CONFIG.depth = (isNaN(v) || v < 0) ? 1 : v;
    computeRenderSets();
    layoutTables();
  });
}

// ----- Pan (drag background) & Zoom (Ctrl/Alt+wheel) -----
const viewport = document.getElementById('viewport');
let isPanning = false; let panStart = {x:0, y:0, sl:0, st:0};
viewport.addEventListener('mousedown', (e)=>{
  if (e.button !== 0) return; // left only
  if (e.target.closest('.table-node')) return; // don't pan when starting on a card
  isPanning = true;
  panStart = { x: e.clientX, y: e.clientY, sl: viewport.scrollLeft, st: viewport.scrollTop };
  viewport.style.cursor = 'grabbing';
});
window.addEventListener('mousemove', (e)=>{
  if (!isPanning) return;
  viewport.scrollLeft = panStart.sl - (e.clientX - panStart.x);
  viewport.scrollTop  = panStart.st - (e.clientY - panStart.y);
  drawEdges();
});
window.addEventListener('mouseup', ()=>{ if (isPanning){ isPanning=false; viewport.style.cursor=''; } });

viewport.addEventListener('wheel', (e)=>{
  if (!(e.ctrlKey || e.metaKey || e.altKey)) return; // only zoom with modifiers
  e.preventDefault();
  const prev = SCALE;
  const factor = (e.deltaY < 0) ? 1.1 : 0.9;
  SCALE = Math.max(0.4, Math.min(2.5, SCALE * factor));
  const stage = document.getElementById('stage');
  stage.style.transform = `scale(${SCALE})`;
  updateWiresSize();

  // Keep cursor position stable during zoom
  const rect = viewport.getBoundingClientRect();
  const mx = e.clientX - rect.left; const my = e.clientY - rect.top;
  const worldX = (viewport.scrollLeft + mx) / prev;
  const worldY = (viewport.scrollTop + my) / prev;
  const newScrollLeft = worldX * SCALE - mx;
  const newScrollTop  = worldY * SCALE - my;
  viewport.scrollLeft = newScrollLeft;
  viewport.scrollTop  = newScrollTop;

  // Redraw with new scale (centerOf divides by SCALE)
  drawEdges();
}, { passive: false });

// ---- Toolbar: Fit / Zoom +/- / Search ----
function zoomBy(factor){
  const prev = SCALE;
  SCALE = Math.max(0.4, Math.min(2.5, SCALE * factor));
  const stage = document.getElementById('stage');
  stage.style.transform = `scale(${SCALE})`;
  updateWiresSize();
  const viewport = document.getElementById('viewport');
  const mx = viewport.clientWidth/2, my = viewport.clientHeight/2;
  const worldX = (viewport.scrollLeft + mx) / prev;
  const worldY = (viewport.scrollTop + my) / prev;
  viewport.scrollLeft = worldX * SCALE - mx;
  viewport.scrollTop = worldY * SCALE - my;
  drawEdges();
}

function fitToContent(){
  const viewport = document.getElementById('viewport');
  const stage = document.getElementById('stage');
  const cards = Array.from(stage.querySelectorAll('.table-node'));
  if (!cards.length) return;
  // content bounds
  let minX=Infinity, minY=Infinity, maxX=-Infinity, maxY=-Infinity;
  cards.forEach(c=>{
    const x = parseFloat(c.style.left||'0');
    const y = parseFloat(c.style.top||'0');
    const w = c.offsetWidth, h=c.offsetHeight;
    minX = Math.min(minX, x); minY = Math.min(minY, y);
    maxX = Math.max(maxX, x+w); maxY = Math.max(maxY, y+h);
  });
  const pad = 120;
  const contentW = (maxX - minX) + pad;
  const contentH = (maxY - minY) + pad;
  const scaleX = viewport.clientWidth / contentW;
  const scaleY = viewport.clientHeight / contentH;
  SCALE = Math.max(0.4, Math.min(1.0, Math.min(scaleX, scaleY)));
  stage.style.transform = `scale(${SCALE})`;
  updateWiresSize();
  // center
  const cx = (minX + maxX)/2 - viewport.clientWidth/(2*SCALE);
  const cy = (minY + maxY)/2 - viewport.clientHeight/(2*SCALE);
  viewport.scrollLeft = Math.max(0, cx);
  viewport.scrollTop = Math.max(0, cy);
  drawEdges();
}

function clearTargets(){
  document.querySelectorAll('.table-node.target').forEach(el=>el.classList.remove('target'));
}

function findAndFocus(q){
  if (!q) return;
  const stage = document.getElementById('stage');
  // Normalize query: trim, lower, strip quotes, plus, and URI prefix
  function cleanQuery(s){
    let x = (s||'').trim().toLowerCase();
    x = x.replace(/^\+|\+$/g,''); // trim + on both ends
    x = x.replace(/^"|"$/g,''); // strip surrounding quotes
    x = x.replace(/^mssql:\/\/[^/]+\//,''); // drop scheme+host
    return x;
  }
  const ql = cleanQuery(q);

  // Try exact column match by fully-qualified key (data-key)
  let li = stage.querySelector(`.table-node li.col-row[data-key="${ql}"]`);
  // Try endsWith match for partial keys like schema.table.column or table.column
  if (!li && ql.includes('.')){
    const needle = '.' + ql;
    li = Array.from(stage.querySelectorAll('.table-node li.col-row')).find(el=>{
      const k = (el.getAttribute('data-key')||'').toLowerCase();
      return k.endsWith(needle);
    });
  }
  // Try column-name contains
  if (!li){
    li = Array.from(stage.querySelectorAll('.table-node li span.name')).find(span=> (span.textContent||'').toLowerCase().includes(ql))?.closest('li');
  }
  if (li){
    const key = li.getAttribute('data-key');
    if (key){ selectColumnKey(key); }
    const card = li.closest('.table-node');
    if (card){
      clearTargets(); card.classList.add('target');
      const viewport = document.getElementById('viewport');
      const rectV = viewport.getBoundingClientRect();
      const rectC = card.getBoundingClientRect();
      const dx = (rectC.left - rectV.left) + rectC.width/2;
      const dy = (rectC.top - rectV.top) + rectC.height/2;
      viewport.scrollLeft += dx - rectV.width/2;
      viewport.scrollTop  += dy - rectV.height/2;
      drawEdges();
      return;
    }
  }

  // Table search: match header text, data-full, or data-id
  let card = Array.from(stage.querySelectorAll('.table-node')).find(art=>{
    const h = art.querySelector('header');
    const label = (h && h.textContent ? h.textContent.toLowerCase() : '');
    const full = (art.getAttribute('data-full')||'');
    const idv  = (art.getAttribute('data-id')||'');
    return label.includes(ql) || full.includes(ql) || idv.includes(ql);
  });
  if (!card) return;
  clearTargets(); card.classList.add('target');
  const viewport = document.getElementById('viewport');
  const rectV = viewport.getBoundingClientRect();
  const rectC = card.getBoundingClientRect();
  const dx = (rectC.left - rectV.left) + rectC.width/2;
  const dy = (rectC.top - rectV.top) + rectC.height/2;
  viewport.scrollLeft += dx - rectV.width/2;
  viewport.scrollTop  += dy - rectV.height/2;
  drawEdges();
}

document.getElementById('btnZoomIn').addEventListener('click', ()=> zoomBy(1.1));
document.getElementById('btnZoomOut').addEventListener('click', ()=> zoomBy(0.9));
document.getElementById('btnFit').addEventListener('click', ()=> fitToContent());
document.getElementById('btnToggleSidebar').addEventListener('click', ()=>{
  const side = document.getElementById('sidebar');
  const res = document.getElementById('sidebarResizer');
  if (!side) return;
  const willCollapse = !side.classList.contains('collapsed');
  if (willCollapse){
    // Save current width and collapse to 0
    const curW = side.offsetWidth || parseInt(side.style.width||'280', 10) || 280;
    try{ localStorage.setItem(SIDEBAR_W_KEY, String(curW)); }catch(_){ }
    side.classList.add('collapsed');
    side.style.width = '0px';
    if (res) res.classList.add('hidden');
  } else {
    // Restore saved width and expand
    const savedW = parseInt(localStorage.getItem(SIDEBAR_W_KEY)||'280', 10) || 280;
    side.classList.remove('collapsed');
    side.style.width = Math.max(160, savedW) + 'px';
    if (res) res.classList.remove('hidden');
  }
  try{ localStorage.setItem(SIDEBAR_KEY, willCollapse ? 'collapsed' : 'expanded'); }catch(_){ }
  // reflow wires due to size change
  setTimeout(()=>{ layoutTables(); }, 100);
});
// Toggle collapse of columns globally
function updateCollapseButton(){
  const b = document.getElementById('btnToggleCollapse');
  if (b) b.textContent = COLLAPSE ? 'Expand' : 'Collapse';
}
document.getElementById('btnToggleCollapse').addEventListener('click', ()=>{
  COLLAPSE = !COLLAPSE;
  try{ localStorage.setItem(COLLAPSE_KEY, COLLAPSE ? '1' : '0'); }catch(_){ }
  try{ clearSelection(); }catch(_){ }
  updateCollapseButton();
  layoutTables();
});
document.getElementById('search').addEventListener('keydown', (e)=>{
  if (e.key === 'Enter'){
    const q = (e.currentTarget.value||'');
    highlightSearch(q);
  }
});

// ===== Context menu (right-click on column row) =====
const ctxMenu = document.getElementById('ctxMenu');
function openCtx(x,y, items){
  if (!ctxMenu) return;
  ctxMenu.innerHTML = '';
  items.forEach(it=>{
    if (it === 'sep'){ const s=document.createElement('div'); s.className='ctx-sep'; ctxMenu.appendChild(s); return; }
    const d=document.createElement('div'); d.className='ctx-item'; d.textContent=it.label; d.tabIndex=0;
    d.addEventListener('click', ()=>{ closeCtx(); it.onClick && it.onClick(); });
    d.addEventListener('keydown', (ev)=>{ if (ev.key==='Enter'||ev.key===' ') { ev.preventDefault(); closeCtx(); it.onClick && it.onClick(); }});
    ctxMenu.appendChild(d);
  });
  ctxMenu.style.left = x+'px'; ctxMenu.style.top = y+'px';
  ctxMenu.style.display = 'block'; ctxMenu.setAttribute('aria-hidden','false');
}
function closeCtx(){ if (ctxMenu){ ctxMenu.style.display='none'; ctxMenu.setAttribute('aria-hidden','true'); } }
document.addEventListener('click', ()=> closeCtx());
document.addEventListener('keydown', (e)=>{ if (e.key==='Escape'){ closeCtx(); /* keep isolation until explicitly cleared via menu */ } });
document.getElementById('stage').addEventListener('contextmenu', (e)=>{
  const li = e.target && e.target.closest('li.col-row');
  if (!li) return;
  e.preventDefault();
  const key = li.getAttribute('data-key'); if (!key) return;
  const px = e.clientX, py = e.clientY;
  openCtx(px, py, [
    { label: 'Show downstream (attribute)', onClick: ()=>{ ISOLATE=true; ISOLATE_DIR='down'; ISOLATE_SRC=key; SELECTED_COL=key; highlightLineage(key,'down',true); drawEdges(); } },
    { label: 'Show upstream (attribute)',   onClick: ()=>{ ISOLATE=true; ISOLATE_DIR='up';   ISOLATE_SRC=key; SELECTED_COL=key; highlightLineage(key,'up',true);   drawEdges(); } },
    { label: 'Show both (attribute)',       onClick: ()=>{ ISOLATE=true; ISOLATE_DIR='both'; ISOLATE_SRC=key; SELECTED_COL=key; highlightLineage(key,'both',true); drawEdges(); } },
    'sep',
    { label: 'Clear filter',                onClick: ()=>{ clearSelection(); drawEdges(); } }
  ]);
});

// ---- Crossing minimization (barycentric) ----
function orderLayers(layers, graph){
  const maxRank = Math.max(...layers.keys());
  for (let iter=0; iter<2; iter++){
    // forward
    for (let r=1; r<=maxRank; r++){
      const prev = layers.get(r-1) || [];
      const ids = layers.get(r) || [];
      const pos = new Map(prev.map((id,i)=>[id,i]));
      ids.sort((a,b)=>{
        const ba = bary(graph.pred.get(a), pos, ids.indexOf(a));
        const bb = bary(graph.pred.get(b), pos, ids.indexOf(b));
        if (ba === bb) return a.localeCompare(b);
        return ba - bb;
      });
      layers.set(r, ids);
    }
    // backward
    for (let r=maxRank-1; r>=0; r--){
      const next = layers.get(r+1) || [];
      const ids = layers.get(r) || [];
      const pos = new Map(next.map((id,i)=>[id,i]));
      ids.sort((a,b)=>{
        const ba = bary(graph.adj.get(a), pos, ids.indexOf(a));
        const bb = bary(graph.adj.get(b), pos, ids.indexOf(b));
        if (ba === bb) return a.localeCompare(b);
        return ba - bb;
      });
      layers.set(r, ids);
    }
  }
}

function bary(neighSet, posMap, fallback){
  if (!neighSet || neighSet.size === 0) return fallback;
  let sum = 0, cnt = 0;
  neighSet.forEach(n=>{ if (posMap.has(n)){ sum += posMap.get(n); cnt++; } });
  return cnt ? sum / cnt : fallback;
}


// ---- Dragging support ----
let drag = null; // { el, startX, startY, left, top }
function makeDraggable(card){
  card.addEventListener('mousedown', (e)=>{
    // Allow clicking on rows or header button without triggering drag
    if (e.target && (e.target.closest('li') || e.target.closest('.sel-btn'))) return;
    const target = e.currentTarget;
    drag = {
      el: target,
      startX: e.clientX,
      startY: e.clientY,
      left: parseFloat(target.style.left||'0') || 0,
      top: parseFloat(target.style.top||'0') || 0,
    };
    target.classList.add('dragging');
    e.preventDefault();
  });
}

window.addEventListener('mousemove', (e)=>{
  if (!drag) return;
  const dx = e.clientX - drag.startX;
  const dy = e.clientY - drag.startY;
  const nl = drag.left + dx;
  const nt = drag.top + dy;
  drag.el.style.left = nl + 'px';
  drag.el.style.top = nt + 'px';
  // expand stage if needed
  const stage = document.getElementById('stage');
  const rightX = nl + drag.el.offsetWidth;
  const bottomY = nt + drag.el.offsetHeight;
  let changed = false;
  if (rightX + 60 > stage.offsetWidth){ stage.style.width = (rightX + 120) + 'px'; changed = true; }
  if (bottomY + 60 > stage.offsetHeight){ stage.style.height = (bottomY + 120) + 'px'; changed = true; }
  if (changed){
    updateWiresSize();
  }
  if (!window.__rafDrawing){
    window.__rafDrawing = true;
    requestAnimationFrame(()=>{ window.__rafDrawing = false; drawEdges(); });
  }
});

window.addEventListener('mouseup', ()=>{
  if (drag){ drag.el.classList.remove('dragging'); }
  drag = null;
});

// ====== Lineage highlight (per-column) ======

function buildColGraph(){
  COL_OUT = new Map();
  COL_IN = new Map();
  ROW_BY_COL = new Map();
  URI_BY_COL = new Map();
  // map li rows by column key
  document.querySelectorAll('.table-node li').forEach(li=>{
    const left = li.querySelector('.port.left');
    const key = left && left.getAttribute('data-key');
    if (key){
      li.classList.add('col-row');
      li.setAttribute('data-key', key);
      li.setAttribute('tabindex','0');
      // also store column name and full for search
      const nameSpan = li.querySelector('.name');
      const colName = nameSpan ? (nameSpan.textContent||'') : '';
      li.setAttribute('data-name', colName.toLowerCase());
      // full: ns.tbl + '.' + col (same as key)
      li.setAttribute('data-full', key.toLowerCase());
      ROW_BY_COL.set(key, li);
    }
  });
  EDGES.forEach(e=>{
    const s = parseUri(e.from), t = parseUri(e.to);
    const sKey = (s.tableId + '.' + s.col).toLowerCase();
    const tKey = (t.tableId + '.' + t.col).toLowerCase();
    if (!COL_OUT.has(sKey)) COL_OUT.set(sKey, []);
    if (!COL_IN.has(tKey)) COL_IN.set(tKey, []);
    COL_OUT.get(sKey).push(e);
    COL_IN.get(tKey).push(e);
    // Example URIs for search
    if (e.from) URI_BY_COL.set(sKey, (e.from||'').toLowerCase());
    if (e.to) URI_BY_COL.set(tKey, (e.to||'').toLowerCase());
  });
  // attach data-uri onto rows if known
  ROW_BY_COL.forEach((li, key)=>{
    const uri = URI_BY_COL.get(key);
    if (uri) li.setAttribute('data-uri', uri);
  });
}

function onStageClick(e){
  const li = e.target && e.target.closest('li.col-row');
  if (!li){
    // Do not clear selection/isolation on background click
    return;
  }
  const key = li.getAttribute('data-key');
  if (!key) return; selectColumnKey(key);
}

function onStageKeyDown(e){
  if (e.key !== 'Enter' && e.key !== ' ') return;
  const li = e.target && e.target.closest('li.col-row');
  if (!li) return;
  e.preventDefault();
  const key = li.getAttribute('data-key');
  if (!key) return; selectColumnKey(key);
}

function onRowClick(e){
  e.stopPropagation();
  const li = e.currentTarget && e.currentTarget.closest('li.col-row');
  if (!li) return;
  const key = li.getAttribute('data-key');
  if (!key) return; selectColumnKey(key);
}

function selectColumnKey(key){
  // Always clear previous selection before applying a new one.
  // If the same row is clicked again, toggle off selection.
  if (SELECTED_COL === key){
    clearSelection();
    return;
  }
  clearSelection();
  SELECTED_COL = key;
  highlightLineage(key, 'both', ISOLATE);
}

function clearSelection(){
  SELECTED_COL = null;
  ISOLATE = false; ISOLATE_DIR = 'both'; ISOLATE_SRC = null;
  // remove classes
  document.querySelectorAll('.table-node, .table-node li, svg .wire').forEach(el=>{
    el.classList.remove('dim','active','selected','hidden');
  });
}

function highlightLineage(srcKey, dir='both', isolate=false){
  const activeCols = new Set();
  const activeEdges = new Set();
  // Downstream
  if (dir==='down' || dir==='both'){
    const q1 = [srcKey]; const seen1 = new Set([srcKey]);
    while(q1.length){
      const u = q1.shift(); activeCols.add(u);
      const outs = COL_OUT.get(u) || [];
      outs.forEach(e=>{
        const t = parseUri(e.to); const v = (t.tableId + '.' + t.col).toLowerCase();
        activeEdges.add(edgeKey(e));
        if (!seen1.has(v)){ seen1.add(v); q1.push(v); }
      });
    }
  }
  // Upstream
  if (dir==='up' || dir==='both'){
    const q2 = [srcKey]; const seen2 = new Set([srcKey]);
    while(q2.length){
      const u = q2.shift(); activeCols.add(u);
      const ins = COL_IN.get(u) || [];
      ins.forEach(e=>{
        const s = parseUri(e.from); const v = (s.tableId + '.' + s.col).toLowerCase();
        activeEdges.add(edgeKey(e));
        if (!seen2.has(v)){ seen2.add(v); q2.push(v); }
      });
    }
  }
  applyHighlight(srcKey, activeCols, activeEdges, isolate);
}

function applyHighlight(srcKey, colSet, edgeSet, isolate=false){
  // Reset all
  document.querySelectorAll('.table-node, .table-node li, svg .wire').forEach(el=>{
    el.classList.remove('dim','active','selected','hidden');
  });
  // Default: dim everything
  document.querySelectorAll('.table-node').forEach(card=>card.classList.add('dim'));
  document.querySelectorAll('.table-node li').forEach(li=>li.classList.add('dim'));
  document.querySelectorAll('svg .wire').forEach(p=>p.classList.add('dim'));

  // Activate rows and remember their tables
  const tablesActive = new Set();
  colSet.forEach(colKey=>{
    const li = ROW_BY_COL.get(colKey);
    if (li){
      li.classList.remove('dim');
      li.classList.add('active');
      const card = li.closest('.table-node');
      if (card){ card.classList.remove('dim'); tablesActive.add(card.id); }
    }
  });

  // Activate edges
  edgeSet.forEach(ek=>{
    const p = PATH_BY_EDGE.get(ek);
    if (p){ p.classList.remove('dim'); p.classList.add('active'); }
  });

  // Mark selected row distinctly
  const sel = ROW_BY_COL.get(srcKey);
  if (sel){ sel.classList.add('selected'); }
  // Isolation mode: hide non-active instead of dim, hide tables without any active row
  if (isolate){
    document.querySelectorAll('svg .wire.dim').forEach(p=>p.classList.add('hidden'));
    document.querySelectorAll('.table-node li.dim').forEach(li=>li.classList.add('hidden'));
    document.querySelectorAll('.table-node').forEach(card=>{
      const anyActive = !!card.querySelector('li.active, li.selected');
      if (!anyActive){ card.classList.add('hidden'); }
      card.classList.remove('dim');
    });
  }
}

function clearSearchHits(){
  document.querySelectorAll('.table-node.hit, .table-node li.hit').forEach(el=>el.classList.remove('hit'));
}

function highlightSearch(q){
  clearSearchHits();
  if (!q){ drawEdges(); return; }
  // Normalize similarly to findAndFocus
  function cleanQuery(s){
    let x = (s||'').trim().toLowerCase();
    x = x.replace(/^\+|\+$/g,'');
    x = x.replace(/^"|"$/g,'');
    x = x.replace(/^mssql:\/\/[^\/]+\//,'');
    return x;
  }
  const ql = cleanQuery(q);
  const stage = document.getElementById('stage');
  // match tables by label/full/id
  const cards = Array.from(stage.querySelectorAll('.table-node')).filter(art=>{
    const label = art.getAttribute('data-label')||'';
    const full = art.getAttribute('data-full')||'';
    const id = art.getAttribute('data-id')||'';
    return label.includes(ql) || full.includes(ql) || id.includes(ql);
  });
  cards.forEach(c=> c.classList.add('hit'));
  // match rows by name/full/uri
  const rows = Array.from(stage.querySelectorAll('.table-node li.col-row')).filter(li=>{
    const name = li.getAttribute('data-name')||'';
    const full = li.getAttribute('data-full')||'';
    const uri = li.getAttribute('data-uri')||'';
    // Support suffix match on fully qualified key
    if (full.endsWith('.'+ql)) return true;
    return name.includes(ql) || full.includes(ql) || uri.includes(ql);
  });
  rows.forEach(li=>{ li.classList.add('hit'); const card = li.closest('.table-node'); if (card) card.classList.add('hit'); });
  // Scroll to first hit
  const first = cards[0] || (rows[0] && rows[0].closest('.table-node'));
  if (first){
    clearTargets(); first.classList.add('target');
    const viewport = document.getElementById('viewport');
    const rectV = viewport.getBoundingClientRect();
    const rectC = first.getBoundingClientRect();
    const dx = (rectC.left - rectV.left) + rectC.width/2;
    const dy = (rectC.top - rectV.top) + rectC.height/2;
    viewport.scrollLeft += dx - rectV.width/2;
    viewport.scrollTop  += dy - rectV.height/2;
  }
  drawEdges();
}
</script>
</body>
</html>
"""


# ---------------- Public API ----------------
def build_viz_html(graph_path: Path, focus=None, depth: int = 2, direction: str = "both") -> str:
    edges = _load_edges(graph_path)
    schema_orders = _load_schema_orders(graph_path)
    tables, e = _build_elements(edges, orders=schema_orders)
    html = HTML_TMPL
    html = html.replace("__NODES__", json.dumps(tables, ensure_ascii=False))
    html = html.replace("__EDGES__", json.dumps(e, ensure_ascii=False))
    html = html.replace("__FOCUS__", json.dumps((focus or "").lower()))
    html = html.replace("__DEPTH__", json.dumps(int(depth)))
    html = html.replace("__DIRECTION__", json.dumps(direction.lower()))
    return html
