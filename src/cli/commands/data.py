import typer
import subprocess
import os
import sys
from rich.console import Console
from rich.table import Table
from typing import Optional
from src.cli.commands import sec

app = typer.Typer(help="Data Ingestion and Standardization")
console = Console()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@app.command()
def ingest(
    source: str = typer.Argument(..., help="Source to ingest: 'universe', 'prices', 'fundamentals', 'sec', 'finra', 'macro', or 'all'"),
    limit: Optional[int] = typer.Option(None, help="Limit number of tickers (for prices/fundamentals)")
):
    """
    Run Bronze Layer ingestion pipelines.
    """
    console.print(f"[bold green]Starting Ingestion for source: {source}[/bold green]")
    
    scripts = []
    
    # 1. Universe (Master CIK List)
    if source == "universe" or source == "all":
        # Uses bronze_data_pipeline.py with 'universe' command
        cmd = ["src/etl/bronze_data_pipeline.py", "universe"]
        scripts.append((cmd[0], cmd[1:]))
    
    # 2. Market Prices (EODHD)
    if source == "prices" or source == "all":
        # Uses specific ingest script
        cmd = ["src/etl/ingest/ingest_market_to_bronze.py"]
        scripts.append((cmd[0], cmd[1:]))
        
    # 3. Fundamentals (SEC Missing + Edgartools)
    if source == "fundamentals" or source == "all":
        # Uses bronze_data_pipeline.py
        cmd = ["src/etl/bronze_data_pipeline.py", "missing"]
        if limit:
             cmd = ["src/etl/bronze_data_pipeline.py", "missing", "--limit", str(limit)]
        scripts.append((cmd[0], cmd[1:]))
        
    # 4. SEC Bulk (Raw Zips)
    if source == "sec" or source == "all":
        # Uses bronze_data_pipeline.py
        cmd = ["src/etl/bronze_data_pipeline.py", "bulk"]
        scripts.append((cmd[0], cmd[1:]))
        
    # 5. FINRA (Short Interest)
    if source == "finra" or source == "all":
        # Uses specific ingest script
        cmd = ["src/etl/ingest/ingest_finra_to_bronze.py"]
        scripts.append((cmd[0], cmd[1:]))
        
    # 6. Macro (FRED) - Currently Missing
    if source == "macro":
        console.print("[bold yellow]Macro ingestion not currently available (Script missing).[/bold yellow]")

    if not scripts and source != "macro" and source != "all":
        console.print(f"[bold red]Unknown source: {source}[/bold red]")
        return

    for script, args in scripts:
        console.print(f"\n[bold blue]Running {script} {' '.join(args)}...[/bold blue]")
        try:
            # Run via subprocess to keep isolation
            full_cmd = [sys.executable, script] + args
            subprocess.run(full_cmd, check=True)
            console.print(f"[bold green]Success: {script}[/bold green]")
        except subprocess.CalledProcessError as e:
            console.print(f"[bold red]Error running {script}: {e}[/bold red]")
            if source != "all":
                 raise typer.Exit(code=1)

@app.command()
def standardize(
    source: str = typer.Argument(..., help="Source to standardize: 'market', 'sec', 'finra', 'macro', or 'all'")
):
    """
    Run Bronze -> Silver Standardizers.
    """
    console.print(f"[bold green]Starting Standardization for source: {source}[/bold green]")
    
    scripts = []
    
    if source == "market" or source == "all":
        scripts.append("src/etl/standardization/standardize_prices.py")
        
    if source == "sec" or source == "all":
        scripts.append("src/etl/standardization/standardize_fundamentals.py")
        
    if source == "finra" or source == "all":
        scripts.append("src/etl/standardization/standardize_finra.py")

    if source == "macro" or source == "all":
        scripts.append("src/etl/standardization/standardize_macro.py")
        
    if not scripts and source != "all":
        console.print(f"[bold red]Unknown source: {source}[/bold red]")
        return

    for script in scripts:
        console.print(f"\n[bold blue]Running {script}...[/bold blue]")
        try:
            subprocess.run([sys.executable, script], check=True)
            console.print(f"[bold green]Success: {script}[/bold green]")
        except subprocess.CalledProcessError as e:
            console.print(f"[bold red]Error running {script}: {e}[/bold red]")
            if source != "all":
                 raise typer.Exit(code=1)
                 
@app.command()
def status():
    """
    Show status of Data Layers (Bronze, Silver, Gold).
    """
    table = Table(title="Data Lakehouse Status")
    table.add_column("Layer", style="cyan")
    table.add_column("Path", style="magenta")
    table.add_column("File Count", style="green")
    table.add_column("Size (Approximated)", style="yellow")
    
    layers = [
        ("Bronze (Raw)", "bronze"),
        ("Silver (Standardized)", "silver"),
        ("Gold (Features)", "gold")
    ]
    
    for name, relative_path in layers:
        path = os.path.join(BASE_DIR, relative_path)
        if not os.path.exists(path):
            table.add_row(name, relative_path, "N/A", "N/A")
            continue
            
        # Quick Count using os.walk
        file_count = 0
        total_size = 0
        for root, dirs, files in os.walk(path):
            file_count += len(files)
            for f in files:
                fp = os.path.join(root, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        
        size_str = f"{total_size / (1024**3):.2f} GB" if total_size > 1024**3 else f"{total_size / (1024**2):.2f} MB"
        table.add_row(name, relative_path, str(file_count), size_str)
        
    console.print(table)

# Add SEC subcommand
app.add_typer(sec.app, name="sec", help="SEC Data Pipeline Operations")
