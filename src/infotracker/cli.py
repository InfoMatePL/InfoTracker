from __future__ import annotations

from .viz import build_viz_html 
import logging
import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import load_config, RuntimeConfig
from .engine import ExtractRequest, ImpactRequest, DiffRequest, Engine
from .io_utils import get_supported_encodings


app = typer.Typer(add_completion=False, no_args_is_help=True, help="InfoTracker CLI")
console = Console()

logging.getLogger("sqlglot").setLevel(logging.ERROR)

def version_callback(value: bool):
    from . import __version__

    if value:
        console.print(f"infotracker {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(None, exists=True, dir_okay=False, help="Path to infotracker.yml"),
    log_level: str = typer.Option("info", help="log level: debug|info|warn|error"),
    format: str = typer.Option("text", "--format", help="Output format: text|json", show_choices=True),
    version: bool = typer.Option(False, "--version", callback=version_callback, is_eager=True, help="Show version and exit"),
):
    ctx.ensure_object(dict)
    cfg = load_config(config)
    # override with CLI flags (precedence)
    cfg.log_level = log_level
    cfg.output_format = format
    ctx.obj["cfg"] = cfg
    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(level=level)


@app.command()
def extract(
    ctx: typer.Context,
    sql_dir: Optional[Path] = typer.Option(None, exists=True, file_okay=False),
    out_dir: Optional[Path] = typer.Option(None, file_okay=False),
    adapter: Optional[str] = typer.Option(None),
    catalog: Optional[Path] = typer.Option(None, exists=True),
    fail_on_warn: bool = typer.Option(False),
    include: list[str] = typer.Option([], "--include", help="Glob include pattern"),
    exclude: list[str] = typer.Option([], "--exclude", help="Glob exclude pattern"),
    encoding: str = typer.Option("auto", "--encoding", "-e", help="File encoding for SQL files", show_choices=True),
):
    cfg: RuntimeConfig = ctx.obj["cfg"]
    
    # Validate encoding
    supported = get_supported_encodings()
    if encoding not in supported:
        console.print(f"[red]ERROR: Unsupported encoding '{encoding}'. Supported: {', '.join(supported)}[/red]")
        raise typer.Exit(1)
    
    engine = Engine(cfg)
    req = ExtractRequest(
        sql_dir=sql_dir or Path(cfg.sql_dir),
        out_dir=out_dir or Path(cfg.out_dir),
        adapter=adapter or cfg.default_adapter,
        catalog=catalog,
        include=include or cfg.include,
        exclude=exclude or cfg.exclude,
        fail_on_warn=fail_on_warn,
        encoding=encoding,
    )
    result = engine.run_extract(req)
    _emit(result, cfg.output_format)
    
    # Handle fail_on_warn
    if fail_on_warn and result.get("warnings", 0) > 0:
        console.print(f"[red]ERROR: {result['warnings']} warnings detected with --fail-on-warn enabled[/red]")
        raise typer.Exit(1)


@app.command()
def impact(
    ctx: typer.Context,
    selector: str = typer.Option(..., "-s", "--selector", help="[+]db.schema.object.column[+] - use + to indicate direction"),
    max_depth: Optional[int] = typer.Option(None),
    out: Optional[Path] = typer.Option(None),
    graph_dir: Optional[Path] = typer.Option(None, "--graph-dir", help="Directory containing column_graph.json"),
):
    cfg: RuntimeConfig = ctx.obj["cfg"]
    engine = Engine(cfg)
    req = ImpactRequest(selector=selector, max_depth=max_depth or 2, graph_dir=graph_dir)
    result = engine.run_impact(req)
    _emit(result, cfg.output_format, out)


@app.command()
def diff(
    ctx: typer.Context,
    base: Optional[Path] = typer.Option(None, "--base", help="Directory containing base OpenLineage artifacts"),
    head: Optional[Path] = typer.Option(None, "--head", help="Directory containing head OpenLineage artifacts"),
    format: str = typer.Option("text", "--format", help="Output format: text|json"),
    threshold: Optional[str] = typer.Option(None, "--threshold", help="Severity threshold: NON_BREAKING|POTENTIALLY_BREAKING|BREAKING"),
):
    """Compare two sets of OpenLineage artifacts for breaking changes."""
    cfg: RuntimeConfig = ctx.obj["cfg"]
    engine = Engine(cfg)
    
    if not base or not head:
        console.print("[red]ERROR: Both --base and --head directories are required[/red]")
        raise typer.Exit(1)
    
    # Validate threshold if provided
    if threshold is not None:
        valid_thresholds = ["NON_BREAKING", "POTENTIALLY_BREAKING", "BREAKING"]
        if threshold not in valid_thresholds:
            console.print(f"[red]ERROR: Invalid threshold '{threshold}'. Must be one of: {', '.join(valid_thresholds)}[/red]")
            raise typer.Exit(1)
    
    result = engine.run_diff(base, head, format, threshold=threshold)
    _emit(result, format)
    raise typer.Exit(code=result.get("exit_code", 0))


@app.command()
def viz(
    ctx: typer.Context,
    graph_dir: Path = typer.Option(..., "--graph-dir", exists=True, file_okay=False,
                                   help="Folder z column_graph.json"),
    out: Optional[Path] = typer.Option(None, "--out", help="Ścieżka do wyjściowego HTML; domyślnie <graph_dir>/lineage_viz.html"),
    focus: Optional[str] = typer.Option(None, "-f", "--focus", help="Punkt startowy (ns.table.column lub wzorzec)"),
    depth: int = typer.Option(2, "--depth", help="Zasięg sąsiadów (poziomy)"),
    direction: str = typer.Option("both", "--direction", help="up|down|both", show_choices=True),
    open_browser: bool = typer.Option(True, "--open", help="Otwórz w przeglądarce po wygenerowaniu"),
):
    cfg: RuntimeConfig = ctx.obj["cfg"]

    graph_path = graph_dir / "column_graph.json"
    if not graph_path.exists():
        console.print(f"[red]Brak pliku: {graph_path}[/red]")
        raise typer.Exit(1)

    out_file = out or (graph_dir / "lineage_viz.html")
    out_file.write_text(
        build_viz_html(graph_path, focus=focus, depth=depth, direction=direction),
        encoding="utf-8"
    )

    console.print(f"[green]Wygenerowano: {out_file}[/green]")
    if open_browser:
        import webbrowser
        webbrowser.open(out_file.resolve().as_uri())

def _emit(payload: dict, fmt: str, out_path: Optional[Path] = None) -> None:
    from rich.table import Table
    from rich.console import Console
    import json

    console = Console()

    if fmt == "json":
        content = json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        # fmt == "text" - we'll capture the table as a string
        table = Table(show_header=True, header_style="bold")
        cols = payload.get("columns", [])
        for k in cols:
            table.add_column(str(k))

        for r in payload.get("rows", []):
            if isinstance(r, dict):
                table.add_row(*[str(r.get(c, "")) for c in cols])
            else:
                # lista / krotka — dopasuj po pozycji
                table.add_row(*[str(x) for x in (list(r) + [""] * max(0, len(cols) - len(r)))][:len(cols)])

        if out_path:
            # Capture table as string for file output
            from io import StringIO
            string_io = StringIO()
            temp_console = Console(file=string_io, width=120)
            temp_console.print(table)
            content = string_io.getvalue()
        else:
            # Print to stdout
            console.print(table)
            return

    # Write to file if out_path is specified
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding='utf-8')
        console.print(f"[green]Output written to {out_path}[/green]")
    else:
        # Print to stdout for JSON format
        if fmt == "json":
            console.print_json(content)



def entrypoint() -> None:
    app()


if __name__ == "__main__":
    entrypoint()

