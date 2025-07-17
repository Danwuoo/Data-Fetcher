import shutil
from pathlib import Path
from typing import Type

import json
import pandas as pd
import typer
import yaml


from backtest_data_module.backtesting.execution import (
    Execution,
    FlatCommission,
    GaussianSlippage,
)
from backtest_data_module.backtesting.orchestrator import Orchestrator
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.strategy import StrategyBase
from backtest_data_module.data_handler import DataHandler
from backtest_data_module.data_processing.cross_validation import walk_forward_split
from backtest_data_module.data_storage import (
    Catalog,
    CatalogEntry,
    HybridStorageManager,
)
from backtest_data_module.reporting.report import ReportGen

SNAPSHOT_DIR = Path("snapshots")
RESTORE_DIR = Path("restored")


def restore_snapshot(snapshot: Path) -> None:
    """還原指定的 snapshot 檔案。"""
    if not snapshot.exists():
        raise FileNotFoundError(snapshot)
    RESTORE_DIR.mkdir(exist_ok=True)
    shutil.unpack_archive(str(snapshot), str(RESTORE_DIR))


def verify_latest() -> None:
    """尋找並還原最新的 snapshot。"""
    snapshots = sorted(
        SNAPSHOT_DIR.glob("*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not snapshots:
        typer.echo("No snapshots found")
        return
    latest = snapshots[0]
    typer.echo(f"Restoring {latest.name}...")
    restore_snapshot(latest)
    typer.echo("Restore complete")


app = typer.Typer(help="ZXQuant CLI tool")

audit_app = typer.Typer(help="Audit related commands")
app.add_typer(audit_app, name="audit")

storage_app = typer.Typer(help="Storage related commands")
app.add_typer(storage_app, name="storage")

backup_app = typer.Typer(help="Backup and restore commands")
app.add_typer(backup_app, name="backup")

orchestrator_app = typer.Typer(help="Orchestrator commands")
app.add_typer(orchestrator_app, name="orchestrator")


@audit_app.command()
def trace(table: str, db: str = ":memory:") -> None:
    """從 Catalog 讀取資料表所在層級與位置。"""
    catalog = Catalog(db_path=db)
    entry: CatalogEntry | None = catalog.get(table)
    if not entry:
        typer.echo(f"Table {table} not found")
        raise typer.Exit(code=1)

    msg = (
        f"Table {entry.table_name} is at {entry.tier} (location: {entry.location})\n"
        f"Schema: {entry.schema_hash}"
    )
    typer.echo(msg)


@app.command()
def walk_forward(
    samples: int,
    train_size: int,
    test_size: int,
    step_size: int,
) -> None:
    """Walk-Forward 資料切分。"""
    for train_idx, test_idx in walk_forward_split(
        samples, train_size, test_size, step_size
    ):
        typer.echo(f"{train_idx} {test_idx}")


@storage_app.command()
def migrate(
    table: str = typer.Option(..., "--table", help="Table to move"),
    to: str = typer.Option(..., "--to", help="Target tier"),
    db: str = typer.Option(":memory:", "--db", help="Catalog location"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show expected action only"),
) -> None:
    """將表格搬移至指定層級。"""
    manager = HybridStorageManager(catalog=Catalog(db_path=db))
    entry = manager.catalog.get(table)
    if entry is None:
        typer.echo(f"Table {table} not found")
        raise typer.Exit(code=1)
    if dry_run:
        typer.echo(f"Will move {table} from {entry.tier} to {to}")
    else:
        manager.migrate(table, entry.tier, to)
        typer.echo(f"Moved {table} from {entry.tier} to {to}")


@backup_app.command()
def verify(latest: bool = False) -> None:
    """驗證或還原備份。"""
    if latest:
        verify_latest()
    else:
        typer.echo("Please use the --latest parameter")


def _get_strategy_cls(strategy_name: str) -> Type[StrategyBase]:
    # This is a simple way to get the strategy class.
    # In a real application, you would have a more robust mechanism
    # for discovering and loading strategies.
    if strategy_name == "SmaCrossover":
        from backtest_data_module.backtesting.strategies.sma_crossover import (
            SmaCrossover,
        )

        return SmaCrossover
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")


def _run_orchestrator(config_path: Path, use_ray: bool):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # This is a sample dataframe. In a real application, you would load
    # data from the DataHandler.
    data = pd.DataFrame(
        {
            "asset": ["AAPL"] * 1000,
            "close": [100 + i + (i % 5) * 5 for i in range(1000)],
        }
    )
    data["date"] = pd.to_datetime(pd.date_range(start="2020-01-01", periods=1000))
    data = data.set_index("date")

    storage_manager = HybridStorageManager({})
    data_handler = DataHandler(storage_manager)
    strategy_cls = _get_strategy_cls(config["strategy_cls"])

    orchestrator = Orchestrator(
        data_handler=data_handler,
        strategy_cls=strategy_cls,
        portfolio_cls=Portfolio,
        execution_cls=lambda: Execution(
            commission_model=FlatCommission(0.001),
            slippage_model=GaussianSlippage(0, 0.001),
        ),
        performance_cls=Performance,
    )

    if use_ray:
        orchestrator.run_ray(config, data)
    else:
        orchestrator.run(config, data)

    output_file = f"{config['run_id']}_results.json"
    orchestrator.to_json(output_file)
    orchestrator.generate_reports()
    typer.echo(f"Backtest complete. Results saved to {output_file}")


@orchestrator_app.command("run-wfa")
def run_wfa(
    config_path: Path = typer.Option(
        ..., "--config", help="Path to the walk-forward config file"
    ),
    use_ray: bool = typer.Option(
        True, "--ray/--no-ray", help="Use Ray for parallel execution"
    ),
):
    """執行 Walk-Forward 分析。"""
    _run_orchestrator(config_path, use_ray)


@orchestrator_app.command("run-cpcv")
def run_cpcv(
    config_path: Path = typer.Option(
        ..., "--config", help="Path to the CPCV config file"
    ),
    use_ray: bool = typer.Option(
        True, "--ray/--no-ray", help="Use Ray for parallel execution"
    ),
):
    """執行 Combinatorial Purged Cross-Validation。"""
    _run_orchestrator(config_path, use_ray)


report_app = typer.Typer(help="Report generation commands")
app.add_typer(report_app, name="report")


@report_app.command("generate")
def generate_report(
    run_id: str = typer.Option(..., "--run-id", help="Run ID of the backtest"),
    fmt: str = typer.Option("pdf", "--fmt", help="Output format (pdf, json)"),
):
    """為指定 run ID 產生報告。"""
    results_path = Path(f"{run_id}_results.json")
    if not results_path.exists():
        typer.echo(f"Results file for run ID '{run_id}' not found.")
        raise typer.Exit(code=1)

    with open(results_path) as f:
        results = json.load(f)

    report_gen = ReportGen(run_id, results)

    if fmt == "pdf":
        output_file = Path(f"report_{run_id}.pdf")
        report_gen.generate_pdf(output_file)
        typer.echo(f"PDF report generated at {output_file}")
    elif fmt == "json":
        output_file = Path(f"report_{run_id}.json")
        json_output = report_gen.generate_json()
        with open(output_file, "w") as f:
            f.write(json_output)
        typer.echo(f"JSON report generated at {output_file}")
    else:
        typer.echo(f"Unknown format: {fmt}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
