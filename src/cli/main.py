import typer
from rich.console import Console
from rich.panel import Panel
from src.cli.commands import data, features, model, system

app = typer.Typer(
    name="ds",
    help="DataStore CLI - Manage your Financial Data Lakehouse",
    add_completion=True,
)

console = Console()

# Register Sub-Commands
app.add_typer(data.app, name="data", help="Ingest and Standardize Data")
app.add_typer(features.app, name="features", help="Manage Feature Store")
app.add_typer(model.app, name="model", help="Train and Verify Models")
app.add_typer(system.app, name="system", help="System Diagnostics & Config")

@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """
    DataStore CLI Entry Point.
    """
    if ctx.invoked_subcommand is None:
        console.print(Panel.fit(
            "[bold blue]DataStore CLI[/bold blue]\n\n"
            "Manage your financial data lakehouse with ease.\n"
            "Run [bold green]./ds --help[/bold green] for commands.",
            title="Welcome",
            border_style="blue"
        ))

if __name__ == "__main__":
    app()
