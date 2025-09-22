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
    nodes = {}
    cy_edges = []

    for e in edges:
        f_ns, f_tbl, f_col = e["from"].rsplit(".", 2)
        t_ns, t_tbl, t_col = e["to"].rsplit(".", 2)

        f_id = _make_id(f_ns, f_tbl, f_col)
        t_id = _make_id(t_ns, t_tbl, t_col)

        # węzły
        if f_id not in nodes:
            nodes[f_id] = {
                "data": {
                    "id": f_id,
                    "label": f_col,
                    "table": f_tbl,
                    "ns": f_ns,
                    "full": f"{f_ns}.{f_tbl}.{f_col}"
                }
            }
        if t_id not in nodes:
            nodes[t_id] = {
                "data": {
                    "id": t_id,
                    "label": t_col,
                    "table": t_tbl,
                    "ns": t_ns,
                    "full": f"{t_ns}.{t_tbl}.{t_col}"
                }
            }

        # krawędź kierunkowa
        cy_edges.append({
            "data": {
                "source": f_id,
                "target": t_id,
                "type": e.get("transformation", "IDENTITY"),
                "desc": e.get("description", "")
            }
        })

    return list(nodes.values()), cy_edges

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

function applyLayout(cy, roots) {
  cy.layout({
    name: 'fcose',
    animate: false,
    gravity: 1.0,
    idealEdgeLength: 80,
    nodeSeparation: 60
  }).run();
  if (roots && roots.length) { cy.fit( cy.collection(roots), 60 ); }
  else { cy.fit( cy.elements(), 40 ); }
}

const cy = cytoscape({
  container: document.getElementById('cy'),
  elements: NODES.concat(EDGES),
  style: [
    { selector: 'node', style: {
        'content': 'data(label)',
        'font-size': 10,
        'text-wrap': 'wrap',
        'text-max-width': 120,
        'background-color': '#9ecae1',
        'border-width': 1,
        'border-color': '#6baed6'
    }},
    { selector: 'edge', style: {
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
        'width': 1
    }},
    { selector: 'edge[type = "IDENTITY"]',   style: { 'line-style':'solid' } },
    { selector: 'edge[type = "CAST"]',       style: { 'line-style':'dashed' } },
    { selector: 'edge[type = "EXPRESSION"]', style: { 'line-style':'dotted' } },
    { selector: '.faded',  style: { 'opacity': 0.08 } },
    { selector: '.hidden', style: { 'display': 'none' } }
  ]
});

function bfsFilter(rootId, depth, direction) {
  cy.elements().removeClass('faded hidden');
  if (!rootId) { applyLayout(cy); return; }

  const root = cy.$id(rootId);
  if (root.empty()) { return; }

  const dir = (direction || 'both').toLowerCase();
  let frontier = [root];
  let visited = new Set([root.id()]);
  let keep = new Set([root.id()]);

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

  cy.elements().forEach(e => { if (!keep.has(e.id())) e.addClass('faded'); });
  applyLayout(cy, [root]);
}

function searchFilter(q) {
  cy.elements().removeClass('hidden faded');
  q = (q || '').toLowerCase();
  if (!q) { applyLayout(cy); return; }
  cy.nodes().forEach(n => {
    const ok = (n.data('full') + ' ' + n.data('table')).toLowerCase().includes(q);
    if (!ok) n.addClass('hidden');
  });
  cy.edges().forEach(e => {
    if (e.source().hasClass('hidden') || e.target().hasClass('hidden')) e.addClass('hidden');
  });
  applyLayout(cy);
}

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
  document.getElementById('search').value = n.data('full');
});

document.getElementById('search').addEventListener('input', (e) => { searchFilter(e.target.value); });
document.getElementById('depth').addEventListener('change', (e) => {
  const dir = document.querySelector('.pill.active').dataset.dir;
  const selected = cy.$(':selected').id();
  if (selected) bfsFilter(selected, parseInt(e.target.value || 2), dir);
});

applyLayout(cy);

// Auto-focus, jeśli podano
if (__FOCUS__) {
  const hit = cy.nodes().filter(n => n.data('full').toLowerCase().includes(__FOCUS__)).first();
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