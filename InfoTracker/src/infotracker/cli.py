from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import load_config, RuntimeConfig
from .engine import ExtractRequest, ImpactRequest, DiffRequest, Engine


app = typer.Typer(add_completion=False, no_args_is_help=True, help="InfoTracker CLI")
console = Console()


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
    format: str = typer.Option("text", help="output format: text|json"),
    version: bool = typer.Option(False, "--version", callback=version_callback, is_eager=True, help="Show version and exit"),
):
    ctx.ensure_object(dict)
    cfg = load_config(config)
    # override with CLI flags (precedence)
    cfg.log_level = log_level
    cfg.output_format = format
    ctx.obj["cfg"] = cfg


@app.command()
def extract(
    ctx: typer.Context,
    sql_dir: Optional[Path] = typer.Option(None, exists=True, file_okay=False),
    out_dir: Optional[Path] = typer.Option(None, file_okay=False),
    adapter: Optional[str] = typer.Option(None),
    catalog: Optional[Path] = typer.Option(None, exists=True),
    fail_on_warn: bool = typer.Option(False),
    include: Optional[str] = typer.Option(None, help="Glob include pattern"),
    exclude: Optional[str] = typer.Option(None, help="Glob exclude pattern"),
):
    cfg: RuntimeConfig = ctx.obj["cfg"]
    engine = Engine(cfg)
    req = ExtractRequest(
        sql_dir=sql_dir or Path(cfg.sql_dir),
        out_dir=out_dir or Path(cfg.out_dir),
        adapter=adapter or cfg.default_adapter,
        catalog=catalog,
        include=include or (cfg.include[0] if cfg.include else None),
        exclude=exclude or (cfg.exclude[0] if cfg.exclude else None),
        fail_on_warn=fail_on_warn,
    )
    result = engine.run_extract(req)
    _emit(result, cfg.output_format)


@app.command()
def impact(
    ctx: typer.Context,
    selector: str = typer.Option(..., "-s", "--selector", help="[+]db.schema.object.column[+]"),
    direction: str = typer.Option("downstream", case_sensitive=False),
    max_depth: Optional[int] = typer.Option(None),
    out: Optional[Path] = typer.Option(None),
):
    cfg: RuntimeConfig = ctx.obj["cfg"]
    engine = Engine(cfg)
    req = ImpactRequest(selector=selector, direction=direction, max_depth=max_depth)
    result = engine.run_impact(req)
    _emit(result, cfg.output_format, out)


@app.command()
def diff(
    ctx: typer.Context,
    base: str = typer.Option(..., help="git ref name for base"),
    head: str = typer.Option(..., help="git ref name for head"),
    sql_dir: Optional[Path] = typer.Option(None, exists=True, file_okay=False),
    adapter: Optional[str] = typer.Option(None),
    severity_threshold: str = typer.Option("BREAKING"),
):
    cfg: RuntimeConfig = ctx.obj["cfg"]
    engine = Engine(cfg)
    req = DiffRequest(
        base=base,
        head=head,
        sql_dir=sql_dir or Path(cfg.sql_dir),
        adapter=adapter or cfg.default_adapter,
        severity_threshold=severity_threshold,
    )
    result = engine.run_diff(req)
    _emit(result, cfg.output_format)
    raise typer.Exit(code=result.get("exit_code", 0))


def _emit(payload: dict, fmt: str, out_path: Optional[Path] = None) -> None:
    if fmt == "json":
        text = json.dumps(payload, indent=2, ensure_ascii=False)
        if out_path:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(text)
        else:
            sys.stdout.write(text + "\n")
        return

    # text format (minimal placeholder)
    if "rows" in payload and isinstance(payload["rows"], list):
        table = Table(show_header=True)
        for k in payload.get("columns", []):
            table.add_column(k)
        for r in payload["rows"]:
            table.add_row(*[str(r.get(c, "")) for c in payload.get("columns", [])])
        console.print(table)
    else:
        console.print(payload)


def entrypoint() -> None:
    app()


if __name__ == "__main__":
    entrypoint()

