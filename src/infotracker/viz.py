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
  }
  html,body{height:100%;margin:0;background:var(--bg);color:var(--text);font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, Arial}
  #viewport{position:relative; height:100%; overflow:auto}
  #stage{position:relative; min-width:100%; min-height:100%; transform-origin: 0 0;}
  svg.wires{position:absolute; inset:0; pointer-events:none; width:100%; height:100%; z-index:20}
  .empty{position:absolute; left:20px; top:20px; color:#6b7280; font-size:14px}
  .table-node{position:absolute; width:240px; background:var(--card); border:1px solid var(--border); border-radius:10px; box-shadow:0 1px 2px rgba(0,0,0,.06)}
  .table-node{ cursor: grab; user-select: none; }
  .table-node.dragging{ box-shadow:0 6px 24px rgba(0,0,0,.18); cursor: grabbing; }
  .table-node header{padding:8px 10px; font-weight:600; color:var(--header-text); background:var(--header); border-bottom:1px solid var(--border); border-radius:10px 10px 0 0; text-align:center}
  .table-node ul{list-style:none; margin:0; padding:6px 10px 10px}
  .table-node li{display:flex; align-items:center; justify-content:center; gap:8px; margin:4px 0; padding:6px 8px; background:var(--row); border:1px solid var(--row-border); border-radius:8px; white-space:nowrap; font-size:13px}
  .table-node li.alt{ background:var(--row-alt) }
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
      const txt = document.createElement('span'); txt.textContent = c;
      const right = document.createElement('span'); right.className='port right';
      const key = `${t.id}.${c}`.toLowerCase();
      left.setAttribute('data-key', key); left.setAttribute('data-side','L');
      right.setAttribute('data-key', key); right.setAttribute('data-side','R');
      li.appendChild(left); li.appendChild(txt); li.appendChild(right);
      ul.appendChild(li);
    });
    art.appendChild(ul);
    stage.appendChild(art);
    cardById.set(t.id, art);
    makeDraggable(art);
  });
  if (wires) stage.appendChild(wires);
  // Sizes
  const maxWidth = Math.max(240, ...[...cardById.values()].map(el=>{
    const w = Math.max(el.querySelector('header').offsetWidth, ...Array.from(el.querySelectorAll('li span:nth-child(2)')).map(s=>s.offsetWidth+60));
    return Math.min(420, w+24);
  }));

  const maxRank = Math.max(...layers.keys());
  let maxRight = 0, maxBottom = 0;
  const centerMap = new Map(); // tableId -> centerY

  for(let rk=0; rk<=maxRank; rk++){
    const x = LEFT + rk*(maxWidth + GUTTER_X);
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

    let currentTop = TOP; // running top, ensures non-overlap
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
    p.setAttribute('marker-end','url(#arrow)');
    svg.appendChild(p);
  });
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
