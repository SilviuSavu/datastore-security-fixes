import typer
import subprocess
import sys
import os
import polars as pl
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Manage Feature Store (Gold Layer)")
console = Console()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@app.command()
def build():
    """
    Build features from Silver -> Gold.
    """
    script = "src/features/build_features.py"
    console.print(f"[bold green]Building Features (Running {script})...[/bold green]")
    try:
        subprocess.run([sys.executable, script], check=True)
        console.print("[bold green]Feature Build Complete.[/bold green]")
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]Feature Build Failed: {e}[/bold red]")
        raise typer.Exit(code=1)

@app.command("list")
def list_features(
    group: str = typer.Option(None, help="Filter by feature group"),
    category: str = typer.Option(None, help="Filter by category")
):
    """
    List registered features from Catalog.
    """
    # Import Client here to avoid circular imports during CLI init if client has issues
    sys.path.insert(0, BASE_DIR)
    from src.feature_store.client import FeatureStore
    
    fs = FeatureStore(base_dir=BASE_DIR)
    df = fs.list_features(group=group, category=category)
    
    if df.height == 0:
        console.print("No features found matching criteria.")
        return
        
    table = Table(title=f"Feature Catalog ({df.height} features)")
    table.add_column("Name", style="cyan")
    table.add_column("Description")
    table.add_column("Group", style="magenta")
    table.add_column("Category", style="green")
    
    for row in df.iter_rows(named=True):
        table.add_row(
            row['name'], 
            row.get('description', ''), 
            row.get('group', ''), 
            row.get('category', '')
        )
        
    console.print(table)

@app.command()
def stats():
    """
    Show quick stats of the Gold Feature Table.
    """
    gold_path = os.path.join(BASE_DIR, "gold", "features")
    if not os.path.exists(gold_path):
        console.print("[red]Gold layer not found.[/red]")
        return
        
    try:
        lf = pl.scan_delta(gold_path)
        schema = lf.collect_schema()
        
        console.print(f"[bold]Gold Layer Location:[/bold] {gold_path}")
        console.print(f"[bold]Total Columns:[/bold] {len(schema)}")
        console.print("[bold]Columns:[/bold]")
        console.print(", ".join(schema.names()))
        
    except Exception as e:
        console.print(f"[red]Error reading Gold stats: {e}[/red]")
