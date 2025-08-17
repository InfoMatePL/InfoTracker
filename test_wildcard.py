#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.infotracker.engine import Engine, ImpactRequest

# Create engine and test wildcard
engine = Engine()
req = ImpactRequest(
    selector="+INFOMART.dbo.fct_sales.*",
    max_depth=2,
    graph_dir="build/lineage"
)

print("Testing wildcard selector:", req.selector)
result = engine.run_impact(req)
print("Result:", result)
