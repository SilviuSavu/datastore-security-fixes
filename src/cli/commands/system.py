import typer
import os
import shutil
import platform
import sys
from rich.console import Console
from rich.panel import Panel

app = typer.Typer(help="System Diagnostics")
console = Console()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@app.command()
def info():
    """
    Show System Environment Info.
    """
    info_str = f"""
    [bold]OS:[/bold] {platform.system()} {platform.release()}
    [bold]Python:[/bold] {sys.version.split()[0]}
    [bold]Base Dir:[/bold] {BASE_DIR}
    """
    console.print(Panel(info_str, title="System Info", expand=False))

@app.command()
def clean(
    target: str = typer.Option("all", help="Target to clean: 'logs', 'cache', 'all'")
):
    """
    Clean logs and temporary files.
    """
    if target in ["logs", "all"]:
        log_dir = os.path.join(BASE_DIR, "logs")
        if os.path.exists(log_dir):
            console.print(f"Cleaning logs in {log_dir}...")
            # For now, maybe just delete files, or keep directory?
            # Let's simple warn for now to be safe
            console.print("[yellow]Log cleaning not fully destructive yet.[/yellow]")
            
    if target in ["cache", "all"]:
        # Find __pycache__
        console.print("Cleaning __pycache__...")
        for root, dirs, files in os.walk(BASE_DIR):
            for d in dirs:
                if d == "__pycache__":
                    shutil.rmtree(os.path.join(root, d))
        console.print("[green]Cache cleared.[/green]")
