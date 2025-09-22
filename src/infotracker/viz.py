# viz.py
import json
from pathlib import Path
from html import escape

def _make_id(ns, tbl, col):  # stabilny identyfikator
    return f"{ns}.{tbl}.{col}".lower()

def _load_edges(graph_path: Path):
    data = json.loads(graph_path.read_text(encoding="utf-8"))
    return data.get("edges", [])

def _build_elements(edges):
    """
    Zwraca (nodes, cy_edges) w układzie compound:
    - node kind="table"  (parent)
    - node kind="column" (child, parent=<table_id>)
    - edges: kolumna -> kolumna
    """
    tables = {}   # table_id -> {'data':{...}, 'cols':set()}
    cols   = {}   # col_id   -> node

    def parse_triplet(uri: str):
        ns_tbl, col = uri.rsplit(".", 1)
        ns, tbl = ns_tbl.rsplit(".", 1)
        return ns, tbl, col

    def table_id(ns, tbl):
        return f"{ns}.{tbl}".lower()

    def col_id(ns, tbl, col):
        return f"{ns}.{tbl}.{col}".lower()

    cy_edges = []

    for e in edges:
        f_ns, f_tbl, f_col = parse_triplet(e["from"])
        t_ns, t_tbl, t_col = parse_triplet(e["to"])

        f_tid = table_id(f_ns, f_tbl)
        t_tid = table_id(t_ns, t_tbl)
        f_cid = col_id(f_ns, f_tbl, f_col)
        t_cid = col_id(t_ns, t_tbl, t_col)

        # Tabele (parent nodes)
        if f_tid not in tables:
            tables[f_tid] = {
                "data": {
                    "id": f_tid,
                    "label": f_tbl,
                    "full": f"{f_ns}.{f_tbl}",
                    "kind": "table",
                    "ncols": 0
                }
            }
        if t_tid not in tables:
            tables[t_tid] = {
                "data": {
                    "id": t_tid,
                    "label": t_tbl,
                    "full": f"{t_ns}.{t_tbl}",
                    "kind": "table",
                    "ncols": 0
                }
            }

        # Kolumny (child nodes)
        if f_cid not in cols:
            cols[f_cid] = {
                "data": {
                    "id": f_cid,
                    "label": f_col,
                    "full": f"{f_ns}.{f_tbl}.{f_col}",
                    "table": f_tbl,
                    "ns": f_ns,
                    "kind": "column",
                    "parent": f_tid
                }
            }
            tables[f_tid]["data"]["ncols"] += 1

        if t_cid not in cols:
            cols[t_cid] = {
                "data": {
                    "id": t_cid,
                    "label": t_col,
                    "full": f"{t_ns}.{t_tbl}.{t_col}",
                    "table": t_tbl,
                    "ns": t_ns,
                    "kind": "column",
                    "parent": t_tid
                }
            }
            tables[t_tid]["data"]["ncols"] += 1

        # Krawędź (kolumna -> kolumna)
        cy_edges.append({
            "data": {
                "source": f_cid,
                "target": t_cid,
                "type": e.get("transformation", "IDENTITY"),
                "desc": e.get("description", "")
            }
        })

    nodes = list(tables.values()) + list(cols.values())
    return nodes, cy_edges


HTML_TMPL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>InfoTracker Lineage</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
  html,body { height:100%; margin:0; font-family: Inter, system-ui, Arial; }
  #toolbar { padding: 8px 12px; display:flex; gap:12px; align-items:center; border-bottom:1px solid #eee; }
  #cy { position:absolute; top:48px; bottom:0; left:0; right:0; }
  input[type="text"] { padding:6px 8px; min-width:320px; }
  .pill { padding:6px 10px; border:1px solid #ddd; border-radius:999px; cursor:pointer; user-select:none; }
  .pill.active { background:#efefef; }
  label { font-size: 12px; color:#444; }
</style>
<script src="https://unpkg.com/cytoscape@3/dist/cytoscape.min.js"></script>
<script src="https://unpkg.com/layout-base/layout-base.js"></script>
<script src="https://unpkg.com/cose-base/cose-base.js"></script>
<script src="https://unpkg.com/cytoscape-fcose/cytoscape-fcose.js"></script>
</head>
<body>
<div id="toolbar">
  <input id="search" type="text" placeholder="Szukaj kolumny / tabeli / ns..." />
  <label>Depth: <input id="depth" type="number" min="1" max="10" value="__DEPTH__" /></label>
  <span class="pill" data-dir="up">Up</span>
  <span class="pill" data-dir="down">Down</span>
  <span class="pill active" data-dir="both">Both</span>
  <small id="stats"></small>
</div>
<div id="cy"></div>
<script>
const NODES = __NODES__;
const EDGES = __EDGES__;
const data  = { focus: __FOCUS__, depth: __DEPTH__, direction: __DIRECTION__ };

const cy = cytoscape({
  container: document.getElementById('cy'),
  elements: NODES.concat(EDGES),
  style: [
    // TABELA (parent)
    { selector: 'node[kind = "table"]', style: {
        'content': 'data(label)',
        'font-size': 12,
        'text-valign': 'top',
        'text-halign': 'center',
        'text-margin-y': 6,
        'background-color': '#cce3ba',
        'border-width': 1,
        'border-color': '#7aa35a',
        'shape': 'round-rectangle',
        'padding': '8px',
        'z-compound-depth': 'bottom'  // tło pod dziećmi
    }},
    // KOLUMNA (child)
    { selector: 'node[kind = "column"]', style: {
        'content': 'data(label)',
        'font-size': 11,
        'shape': 'rectangle',
        'background-color': '#fff',
        'border-width': 1,
        'border-color': '#ddd',
        'text-valign': 'center',
        'text-halign': 'left',
        'text-margin-x': 8,
        'width': 180,   /* nadpisywane w layoutTables */
        'height': 18    /* j.w. */
    }},
    // KRAWĘDZIE
    { selector: 'edge', style: {
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
        'width': 1.2,
        'line-color': '#9ca3af',
        'target-arrow-color': '#9ca3af'
    }},
    { selector: 'edge[type = "IDENTITY"]',   style: { 'line-style':'solid' } },
    { selector: 'edge[type = "CAST"]',       style: { 'line-style':'dashed' } },
    { selector: 'edge[type = "EXPRESSION"]', style: { 'line-style':'dotted' } },
    { selector: '.faded',  style: { 'opacity': 0.08 } },
    { selector: '.hidden', style: { 'display': 'none' } }
  ]
});

// --- Layout: tabele układa fcose, kolumny układamy ręcznie w środku rodzica ---
function layoutTables() {
  const ROW_H = 22;         // wysokość wiersza
  const HEADER = 24;        // nagłówek tabeli
  const WIDTH = 200;        // szerokość tabeli
  const PAD = 8;

  cy.batch(() => {
    cy.nodes('[kind = "table"]').forEach(t => {
      const cols = t.children('[kind = "column"]');
      const n = cols.length;
      const H = Math.max(HEADER + n * ROW_H + PAD, 48);

      t.style('width', WIDTH);
      t.style('height', H);

      const tp = t.position();
      // top Y wewnątrz tabeli
      let y = tp.y - H/2 + HEADER + ROW_H/2;

      cols.forEach((c, i) => {
        c.style('width', WIDTH - 16);
        c.style('height', ROW_H - 6);
        c.position({ x: tp.x, y: y + i * ROW_H });
      });
    });
  });
}

function applyLayout(rootNodes) {
  cy.layout({
    name: 'fcose',
    animate: false,
    gravity: 1.0,
    idealEdgeLength: 100,
    nodeSeparation: 80
  }).run();
  layoutTables();
  if (rootNodes && rootNodes.length) cy.fit(cy.collection(rootNodes), 60);
  else cy.fit(cy.elements(), 60);
}

// --- BFS, z obsługą kliknięcia w tabelę (rozszerzamy na jej kolumny) ---
function asFrontier(node) {
  return node.isParent() ? node.children('[kind = "column"]') : cy.collection(node);
}

function bfsFilter(rootId, depth, direction) {
  cy.elements().removeClass('faded hidden');
  if (!rootId) { applyLayout(); return; }

  const rootRaw = cy.$id(rootId);
  if (rootRaw.empty()) return;

  const rootCols = asFrontier(rootRaw); // jeśli tabela, startujemy od kolumn
  const dir = (direction || 'both').toLowerCase();

  let frontier = rootCols.toArray();
  const keep = new Set([rootRaw.id(), ...rootCols.map(n => n.id())]);
  const visited = new Set([...keep]);

  for (let d=0; d<depth; d++) {
    const next = [];
    for (const node of frontier) {
      if (dir==='up' || dir==='both') {
        node.incomers('node, edge').forEach(e => {
          if (e.isEdge()) keep.add(e.id());
          else { if (!visited.has(e.id())) { visited.add(e.id()); next.push(e); } keep.add(e.id()); }
        });
      }
      if (dir==='down' || dir==='both') {
        node.outgoers('node, edge').forEach(e => {
          if (e.isEdge()) keep.add(e.id());
          else { if (!visited.has(e.id())) { visited.add(e.id()); next.push(e); } keep.add(e.id()); }
        });
      }
    }
    frontier = next;
  }

  // pokaż też „rodziców” wszystkich zachowanych kolumn (żeby ramki tabel były widoczne)
  [...keep].forEach(id => {
    const n = cy.getElementById(id);
    if (n.nonempty() && n.parent().nonempty()) keep.add(n.parent().id());
  });

  cy.elements().forEach(e => { if (!keep.has(e.id())) e.addClass('faded'); });

  // fokus na tabelę (jeśli kliknięto w kolumnę, też zadziała)
  applyLayout([rootRaw]);
}

// --- search/depth/direction: zostawiamy jak było ---
document.querySelectorAll('.pill').forEach(p => {
  p.addEventListener('click', () => {
    document.querySelectorAll('.pill').forEach(x => x.classList.remove('active'));
    p.classList.add('active');
    bfsFilter(cy.$(':selected').id(), parseInt(document.getElementById('depth').value || 2), p.dataset.dir);
  });
});

cy.on('tap', 'node', (evt) => {
  const n = evt.target;
  n.select();
  const dir = document.querySelector('.pill.active').dataset.dir;
  bfsFilter(n.id(), parseInt(document.getElementById('depth').value || 2), dir);
  document.getElementById('search').value = n.data('full') || n.data('label');
});

function searchFilter(q) {
  cy.elements().removeClass('hidden faded');
  q = (q || '').toLowerCase();
  if (!q) { applyLayout(); return; }
  cy.nodes().forEach(n => {
    const hay = ((n.data('full') || n.data('label')) + ' ' + (n.data('table') || '')).toLowerCase();
    if (!hay.includes(q)) n.addClass('hidden');
  });
  cy.edges().forEach(e => {
    if (e.source().hasClass('hidden') || e.target().hasClass('hidden')) e.addClass('hidden');
  });
  applyLayout();
}

document.getElementById('search').addEventListener('input', (e) => { searchFilter(e.target.value); });
document.getElementById('depth').addEventListener('change', (e) => {
  const dir = document.querySelector('.pill.active').dataset.dir;
  const selected = cy.$(':selected').id();
  if (selected) bfsFilter(selected, parseInt(e.target.value || 2), dir);
});

// start
applyLayout();

// auto-focus (jak wcześniej)
if (__FOCUS__) {
  const hit = cy.nodes().filter(n => ((n.data('full')||'') + ' ' + (n.data('label')||'')).toLowerCase().includes(__FOCUS__)).first();
  if (hit) {
    hit.select();
    const dir = (__DIRECTION__ || 'both');
    bfsFilter(hit.id(), __DEPTH__ || 2, dir);
  }
}
</script>

</body>
</html>
"""

def build_viz_html(graph_path: Path, focus=None, depth=2, direction="both") -> str:
    edges = _load_edges(graph_path)
    nodes, cy_edges = _build_elements(edges)

    html = HTML_TMPL
    html = html.replace("__NODES__", json.dumps(nodes, ensure_ascii=False))
    html = html.replace("__EDGES__", json.dumps(cy_edges, ensure_ascii=False))
    html = html.replace("__FOCUS__", json.dumps((focus or "").lower()))
    html = html.replace("__DEPTH__", str(int(depth)))
    html = html.replace("__DIRECTION__", json.dumps(direction.lower()))
    return html