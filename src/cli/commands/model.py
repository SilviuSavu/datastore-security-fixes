import typer
import subprocess
import sys
import os
from rich.console import Console

app = typer.Typer(help="Train and Evaluate Models")
console = Console()

@app.command()
def train(
    model_type: str = typer.Argument(..., help="Model type: 'mlx', 'lgbm', or 'hybrid'")
):
    """
    Launch a model training job.
    """
    scripts = {
        "mlx": "src/models/train_mlx.py",
        "lgbm": "src/models/train_lgbm.py",
        "hybrid": "src/models/train_hybrid.py"
    }
    
    if model_type not in scripts:
        console.print(f"[bold red]Unknown model type: {model_type}[/bold red]")
        console.print("Available types: " + ", ".join(scripts.keys()))
        raise typer.Exit(code=1)
        
    script = scripts[model_type]
    console.print(f"[bold green]Starting Training for {model_type.upper()}...[/bold green]")
    try:
        subprocess.run([sys.executable, script], check=True)
        console.print(f"[bold green]Training Complete: {model_type}[/bold green]")
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]Training Failed: {e}[/bold red]")
        raise typer.Exit(code=1)

@app.command()
def verify():
    """
    Run pipeline checks (Placeholder for smoke tests).
    """
    console.print("[yellow]Verification tests not yet implemented.[/yellow]")
