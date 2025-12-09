#!/usr/bin/env python3
"""
SEC Data Pipeline CLI Module
============================

Modern Typer-based CLI for SEC data operations with rich features,
argument validation, and comprehensive help system.
"""

import typer
import subprocess
import sys
import os
from rich.console import Console
from rich.table import Table
from typing import Optional, List
from pathlib import Path

app = typer.Typer(
    name="sec",
    help="SEC Data Pipeline Operations",
    add_completion=True,
    no_args_is_help=True
)

console = Console()

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@app.command()
def complete(
    batch_size: int = typer.Option(100, "--batch-size", help="Batch size for missing CIK downloads"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without executing"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable detailed logging"),
    force: bool = typer.Option(False, "--force", help="Force re-processing even if data exists")
):
    """
    Run the complete SEC data pipeline (bulk processing + missing CIKs + universe creation).
    """
    _run_sec_pipeline("complete", batch_size=batch_size, dry_run=dry_run, verbose=verbose, force=force)

@app.command()
def bulk(
    quarters: Optional[str] = typer.Option(None, "--quarters", help="Specific quarters to process (e.g., '2023q1,2023q2')"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be processed"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable detailed processing logs"),
    force: bool = typer.Option(False, "--force", help="Force re-processing")
):
    """
    Run SEC bulk data processing only.
    """
    _run_sec_pipeline("bulk", quarters=quarters, dry_run=dry_run, verbose=verbose, force=force)

@app.command()
def universe(
    filter_foreign: bool = typer.Option(False, "--filter-foreign", help="Exclude foreign filers (20-F/40-F)"),
    min_market_cap: float = typer.Option(10_000_000, "--min-market-cap", help="Minimum market cap threshold in USD"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show universe creation plan"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable detailed filtering logs")
):
    """
    Run master CIK universe creation only.
    """
    _run_sec_pipeline("universe", filter_foreign=filter_foreign, min_market_cap=min_market_cap,
                     dry_run=dry_run, verbose=verbose)

@app.command()
def missing(
    batch_size: int = typer.Option(100, "--batch-size", help="Download batch size"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of missing CIKs to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be downloaded"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable detailed download logs")
):
    """
    Run missing CIK download only.
    """
    _run_sec_pipeline("missing", batch_size=batch_size, limit=limit, dry_run=dry_run, verbose=verbose)

@app.command()
def status(
    detailed: bool = typer.Option(False, "--detailed", help="Show detailed breakdown by quarter/year")
):
    """
    Show current SEC data status.
    """
    console.print("[bold green]SEC Data Status[/bold green]")

    # Check for SEC bulk data
    sec_bulk_dir = os.path.join(BASE_DIR, "bronze", "sec_bulk")
    sec_zip_files = list(Path(sec_bulk_dir).glob("*.zip")) if os.path.exists(sec_bulk_dir) else []

    # Check for processed data
    raw_output_dir = os.path.join(BASE_DIR, "bronze", "fundamentals", "sec_bulk", "parquet")
    parquet_files = list(Path(raw_output_dir).glob("*.parquet")) if os.path.exists(raw_output_dir) else []

    # Check for master universe files
    master_files = []
    # Check references directory
    ref_dir = os.path.join(BASE_DIR, "references")
    if os.path.exists(ref_dir):
        for f in ["master_cik_list.parquet", "master_cik_universe.parquet", "foreign_stocks.parquet", "cik_universe_summary.json"]:
            if os.path.exists(os.path.join(ref_dir, f)):
                master_files.append(f)

    # Create status table
    table = Table(title="SEC Data Status")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Details", style="green")

    table.add_row("SEC Bulk ZIPs", f"{len(sec_zip_files)} files", ", ".join([f.name for f in sec_zip_files[:3]]) + ("..." if len(sec_zip_files) > 3 else ""))
    table.add_row("Processed Parquet", f"{len(parquet_files)} files", ", ".join([f.name for f in parquet_files[:3]]) + ("..." if len(parquet_files) > 3 else ""))
    table.add_row("Master Universe", f"{len(master_files)} files", ", ".join(master_files))

    console.print(table)

    if detailed:
        _show_detailed_status()

def _show_detailed_status():
    """Show detailed status breakdown"""
    console.print("\n[bold blue]Detailed SEC Data Breakdown[/bold blue]")

    # Show quarter files
    sec_bulk_dir = os.path.join(BASE_DIR, "bronze", "sec_bulk")
    if os.path.exists(sec_bulk_dir):
        zip_files = sorted(Path(sec_bulk_dir).glob("*.zip"))
        if zip_files:
            console.print("\n[bold]SEC Bulk ZIP Files:[/bold]")
            for i, zip_file in enumerate(zip_files, 1):
                size_mb = zip_file.stat().st_size / (1024 * 1024)
                console.print(f"  {i}. {zip_file.name} ({size_mb:.1f} MB)")

    # Show processed parquet files
    raw_output_dir = os.path.join(BASE_DIR, "bronze", "fundamentals", "sec_bulk", "parquet")
    if os.path.exists(raw_output_dir):
        parquet_files = sorted(Path(raw_output_dir).glob("*.parquet"))
        if parquet_files:
            console.print("\n[bold]Processed Parquet Files:[/bold]")
            for i, parquet_file in enumerate(parquet_files, 1):
                size_mb = parquet_file.stat().st_size / (1024 * 1024)
                console.print(f"  {i}. {parquet_file.name} ({size_mb:.1f} MB)")

def _run_sec_pipeline(
    command: str,
    batch_size: int = 100,
    quarters: Optional[str] = None,
    filter_foreign: bool = False,
    min_market_cap: float = 10_000_000,
    limit: Optional[int] = None,
    dry_run: bool = False,
    verbose: bool = False,
    force: bool = False
):
    """
    Run the SEC data pipeline with the specified command and options.
    """
    console.print(f"[bold green]Starting SEC Pipeline: {command}[/bold green]")

    if dry_run:
        console.print("[bold yellow]DRY RUN MODE: No actual processing will occur[/bold yellow]")
        _show_dry_run_plan(command, batch_size, quarters, filter_foreign, min_market_cap, limit, force)
        return

    # Build the command line arguments
    cmd_args = [command]

    if command in ["complete", "missing"]:
        cmd_args.extend(["--batch-size", str(batch_size)])
    if command == "bulk" and quarters:
        cmd_args.extend(["--quarters", quarters])
    if command == "universe":
        if filter_foreign:
            cmd_args.append("--filter-foreign")
        cmd_args.extend(["--min-market-cap", str(min_market_cap)])
    if command == "missing" and limit:
        cmd_args.extend(["--limit", str(limit)])
    if force:
        cmd_args.append("--force")
    if verbose:
        cmd_args.append("--verbose")

    # Construct the full command
    script_path = os.path.join(BASE_DIR, "src", "etl", "bronze_data_pipeline.py")
    full_cmd = [sys.executable, script_path] + cmd_args

    console.print(f"[bold blue]Running: {' '.join(full_cmd)}[/bold blue]")

    try:
        result = subprocess.run(full_cmd, check=True, capture_output=True, text=True)
        console.print(f"[bold green]✅ Success: {command}[/bold green]")
        if result.stdout:
            console.print(f"[green]{result.stdout}[/green]")
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]❌ Error running {command}: {e}[/bold red]")
        if e.stdout:
            console.print(f"[red]{e.stdout}[/red]")
        if e.stderr:
            console.print(f"[red]{e.stderr}[/red]")
        raise typer.Exit(code=1)

def _show_dry_run_plan(
    command: str,
    batch_size: int = 100,
    quarters: Optional[str] = None,
    filter_foreign: bool = False,
    min_market_cap: float = 10_000_000,
    limit: Optional[int] = None,
    force: bool = False
):
    """
    Show what would be executed in dry-run mode.
    """
    console.print("\n[bold blue]DRY RUN PLAN[/bold blue]")
    console.print(f"Command: {command}")

    if command in ["complete", "missing"]:
        console.print(f"Batch Size: {batch_size}")
    if command == "bulk":
        console.print(f"Quarters: {quarters or 'All available'}")
    if command == "universe":
        console.print(f"Filter Foreign: {filter_foreign}")
        console.print(f"Min Market Cap: ${min_market_cap:,.0f}")
    if command == "missing" and limit:
        console.print(f"Limit: {limit}")
    if force:
        console.print("Force Processing: Yes")

    console.print("\nThis would execute:")
    console.print(f"  python src/etl/sec_data_pipeline.py {command} [options]")

if __name__ == "__main__":
    app()