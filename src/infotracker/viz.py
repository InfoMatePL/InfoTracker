"""InfoTracker Column Lineage visualiser (no external libs, DOM+SVG).

This module reads column-level lineage edges and returns a single HTML file
that renders tables as green cards with column rows and draws SVG wires
between the left/right edges of the corresponding rows.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

Edge = Dict[str, str]


# ---------------- I/O ----------------
def _load_edges(graph_path: Path) -> Sequence[Edge]:
    data = json.loads(graph_path.read_text(encoding="utf-8"))
    return data.get("edges", [])


# ---------------- Model ➜ Simple structures ----------------
def _parse_uri(uri: str) -> Tuple[str, str, str]:
    ns_tbl, col = uri.rsplit(".", 1)
    ns, tbl = ns_tbl.rsplit(".", 1)
    return ns, tbl, col


def _table_key(ns: str, tbl: str) -> str:
    return f"{ns}.{tbl}".lower()


def _build_elements(edges: Iterable[Edge]) -> Tuple[List[Dict], List[Dict]]:
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
                    "columns": set(),
                },
            )
            tables[key]["columns"].add(col)

    table_list: List[Dict] = []
    for key, t in tables.items():
        cols = sorted(t["columns"])  # deterministic
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
  }
  html,body{height:100%;margin:0;background:var(--bg);color:var(--text);font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, Arial}
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
  #toolbar input::placeholder{ color:#94a3b8 }
  #toolbar input:focus{ border-color:#60a5fa; box-shadow: 0 0 0 3px rgba(96,165,250,0.25); outline: none }
  
  /* Dark mode adjustments */
  @media (prefers-color-scheme: dark){
    :root{ --bg:#0b1020; --card:#13202b; --card-target:#1a2936; --fx:#273043; --header:#2c7d4d; --header-text:#e8f2e8; --border:#203042; --text:#e5eef5; --row:#132a1f; --row-alt:#0f241b; --row-border:#1f3a2e; --wire:#8da891; --wire-strong:#a2c79f; --sel-bg:#374151; /* slate-700 */ --sel-outline:#e5eef5; }
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
  }
  #viewport{position:relative; height:100%; overflow:auto}
  #stage{position:relative; min-width:100%; min-height:100%; transform-origin: 0 0;}
  svg.wires{position:absolute; inset:0; pointer-events:none; width:100%; height:100%; z-index:20}
  .empty{position:absolute; left:20px; top:20px; color:#6b7280; font-size:14px}
  .empty{ top:80px }
  .table-node{position:absolute; width:240px; background:var(--card); border:1px solid var(--border); border-radius:10px; box-shadow:0 1px 2px rgba(0,0,0,.06)}
  .table-node{ cursor: grab; user-select: none; }
  .table-node.dragging{ box-shadow:0 6px 24px rgba(0,0,0,.18); cursor: grabbing; }
  .table-node header{padding:8px 10px; font-weight:600; color:var(--header-text); background:var(--header); border-bottom:1px solid var(--border); border-radius:10px 10px 0 0; text-align:center}
  .table-node ul{list-style:none; margin:0; padding:6px 10px 10px}
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
  svg .wire{fill:none; stroke:var(--wire-strong); stroke-width:2.4; stroke-linecap:round; stroke-linejoin:round}
  svg .wire.strong{stroke-width:3.2}
  svg defs marker#arrow{ overflow:visible }
</style>
</head>
<body>
<div id="toolbar">
  <button id="btnFit" title="Fit to content">Fit</button>
  <button id="btnZoomOut" title="Zoom out">−</button>
  <button id="btnZoomIn" title="Zoom in">+</button>
  <input id="search" type="text" placeholder="Search table/column… (Enter to jump)" />
</div>
<div id=\"viewport\">
  <div id=\"stage\"></div>
  <svg class=\"wires\" id=\"wires\" aria-hidden=\"true\">
    <defs>
      <marker id=\"arrow\" markerWidth=\"8\" markerHeight=\"8\" refX=\"6\" refY=\"3.5\" orient=\"auto\">
        <polygon points=\"0 0, 7 3.5, 0 7\" fill=\"var(--wire-strong)\"/>
      </marker>
      <!-- colorized arrow markers will be injected below (arrow-0..N) -->
    </defs>
  </svg>
</div>
<script>
const TABLES = __NODES__;
const EDGES = __EDGES__;
const CONFIG = { focus: __FOCUS__, depth: __DEPTH__, direction: __DIRECTION__ };

// Helpers
const ROW_H = 30, GUTTER_Y = 16, GUTTER_X = 260, LEFT = 60, TOP = 60;
// Global scale used by pan/zoom and wire projection; must be defined before first draw
let SCALE = 1;
let FIRST_FIT_DONE = false;

// Lineage highlight globals (declared early to avoid TDZ on first draw)
let COL_OUT = null; // Map colKey -> Array<edge>
let COL_IN = null;  // Map colKey -> Array<edge>
let ROW_BY_COL = new Map(); // colKey -> <li>
let PATH_BY_EDGE = new Map(); // edgeKey -> <path>
let SELECTED_COL = null;

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
  const defs = document.querySelector('#wires defs');
  if (!defs) return;
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
    if (s.tableId!==t.tableId){
      if(!adj.get(s.tableId).has(t.tableId)){
        adj.get(s.tableId).add(t.tableId);
        indeg.set(t.tableId, indeg.get(t.tableId)+1);
        pred.get(t.tableId).add(s.tableId);
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
  // Keep a reference to wires before clearing stage contents
  let wires = document.getElementById('wires');
  // Clear stage content but re-append wires node afterwards
  stage.innerHTML = '';
  
  if (!TABLES || !TABLES.length){
    const info = document.createElement('div'); info.className='empty'; info.textContent = 'No edges found in column_graph.json';
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
    const h = document.createElement('header'); h.textContent = t.label; art.appendChild(h);
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
  if (wires) stage.appendChild(wires);
  // Rows exist now -> (re)build column graph and mark rows clickable
  buildColGraph();
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
  const svg = document.getElementById('wires');
  svg.setAttribute('width', String(stageRectW));
  svg.setAttribute('height', String(stageRectH));
  svg.setAttribute('viewBox', `0 0 ${stageRectW} ${stageRectH}`);
  svg.style.width = stageRectW + 'px';
  svg.style.height = stageRectH + 'px';

  drawEdges();
  requestAnimationFrame(drawEdges);

  // setup click handler for lineage highlight (event delegation)
  stage.onclick = onStageClick;
  stage.onkeydown = onStageKeyDown;

  // auto-fit on first successful layout so users see content immediately
  if (!FIRST_FIT_DONE && TABLES && TABLES.length){
    FIRST_FIT_DONE = true;
    setTimeout(()=>{ try{ fitToContent(); }catch(_){} }, 0);
  }
}

function centerOf(el){
  const r = el.getBoundingClientRect();
  const s = document.getElementById('stage').getBoundingClientRect();
  const x = (r.left - s.left + r.width/2) / SCALE;
  const y = (r.top - s.top + r.height/2) / SCALE;
  return { x, y };
}

function drawEdges(){
  const svg = document.getElementById('wires');
  // clear old
  while(svg.lastChild && svg.lastChild.tagName !== 'defs') svg.removeChild(svg.lastChild);

  PATH_BY_EDGE.clear();
  EDGES.forEach(e=>{
    const s = parseUri(e.from), t = parseUri(e.to);
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
    try { highlightLineage(SELECTED_COL); } catch(_) {}
  }
}

layoutTables();
window.addEventListener('resize', ()=>{ layoutTables(); });
document.getElementById('viewport').addEventListener('scroll', ()=>{ drawEdges(); });

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
  const ql = q.toLowerCase();
  // try table header
  let card = Array.from(stage.querySelectorAll('.table-node')).find(art=>{
    const h = art.querySelector('header');
    return h && h.textContent.toLowerCase().includes(ql);
  });
  // fallback to columns
  if (!card){
    const li = Array.from(stage.querySelectorAll('.table-node li span:nth-child(2)')).find(span=> span.textContent.toLowerCase().includes(ql));
    if (li) card = li.closest('.table-node');
  }
  if (!card) return;
  clearTargets(); card.classList.add('target');
  const viewport = document.getElementById('viewport');
  const rectV = viewport.getBoundingClientRect();
  const rectC = card.getBoundingClientRect();
  // compute desired scroll to center the card
  const dx = (rectC.left - rectV.left) + rectC.width/2;
  const dy = (rectC.top - rectV.top) + rectC.height/2;
  viewport.scrollLeft += dx - rectV.width/2;
  viewport.scrollTop  += dy - rectV.height/2;
  drawEdges();
}

document.getElementById('btnZoomIn').addEventListener('click', ()=> zoomBy(1.1));
document.getElementById('btnZoomOut').addEventListener('click', ()=> zoomBy(0.9));
document.getElementById('btnFit').addEventListener('click', ()=> fitToContent());
document.getElementById('search').addEventListener('keydown', (e)=>{
  if (e.key === 'Enter') findAndFocus(e.currentTarget.value||'');
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
    // Allow clicking on rows without triggering drag
    if (e.target && e.target.closest('li')) return;
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
    const svg = document.getElementById('wires');
    svg.setAttribute('width', String(stage.offsetWidth));
    svg.setAttribute('height', String(stage.offsetHeight));
    svg.setAttribute('viewBox', `0 0 ${stage.offsetWidth} ${stage.offsetHeight}`);
    svg.style.width = stage.offsetWidth + 'px';
    svg.style.height = stage.offsetHeight + 'px';
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
  // map li rows by column key
  document.querySelectorAll('.table-node li').forEach(li=>{
    const left = li.querySelector('.port.left');
    const key = left && left.getAttribute('data-key');
    if (key){ li.classList.add('col-row'); li.setAttribute('data-key', key); li.setAttribute('tabindex','0'); ROW_BY_COL.set(key, li); }
  });
  EDGES.forEach(e=>{
    const s = parseUri(e.from), t = parseUri(e.to);
    const sKey = (s.tableId + '.' + s.col).toLowerCase();
    const tKey = (t.tableId + '.' + t.col).toLowerCase();
    if (!COL_OUT.has(sKey)) COL_OUT.set(sKey, []);
    if (!COL_IN.has(tKey)) COL_IN.set(tKey, []);
    COL_OUT.get(sKey).push(e);
    COL_IN.get(tKey).push(e);
  });
}

function onStageClick(e){
  const li = e.target && e.target.closest('li.col-row');
  if (!li){
    clearSelection();
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
  highlightLineage(key);
}

function clearSelection(){
  SELECTED_COL = null;
  // remove classes
  document.querySelectorAll('.table-node, .table-node li, svg .wire').forEach(el=>{
    el.classList.remove('dim','active','selected');
  });
}

function highlightLineage(srcKey){
  const activeCols = new Set();
  const activeEdges = new Set();
  // BFS downstream
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
  // BFS upstream
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
  applyHighlight(srcKey, activeCols, activeEdges);
}

function applyHighlight(srcKey, colSet, edgeSet){
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
}
</script>
</body>
</html>
"""


# ---------------- Public API ----------------
def build_viz_html(graph_path: Path, focus=None, depth: int = 2, direction: str = "both") -> str:
    edges = _load_edges(graph_path)
    tables, e = _build_elements(edges)
    html = HTML_TMPL
    html = html.replace("__NODES__", json.dumps(tables, ensure_ascii=False))
    html = html.replace("__EDGES__", json.dumps(e, ensure_ascii=False))
    html = html.replace("__FOCUS__", json.dumps((focus or "").lower()))
    html = html.replace("__DEPTH__", json.dumps(int(depth)))
    html = html.replace("__DIRECTION__", json.dumps(direction.lower()))
    return html
