"""
pipeline/run_pipeline.py
=========================
Master pipeline orchestrator — runs all stages in order and stops on failure.

Stages:
  1. Silver Processor  → reads bronze JSON, writes Delta Lake silver tables
  2. Data Quality Gate → runs GX checks, exits 1 on violations
  3. dbt               → builds Gold layer (staging + mart models)

Run:
    python pipeline/run_pipeline.py

For Airflow integration, see pipeline/airflow_dag.py
"""

import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from rich.console import Console
from rich.panel   import Panel
from rich.table   import Table

PROJECT_ROOT = Path(__file__).parent.parent
# Force UTF-8 output so emoji/Unicode don't crash on Windows cp1252 terminals
import io, sys as _sys
if hasattr(_sys.stdout, 'buffer'):
    _sys.stdout = io.TextIOWrapper(_sys.stdout.buffer, encoding='utf-8', errors='replace')
console      = Console()


def run_step(name: str, cmd: list[str], cwd: Path = PROJECT_ROOT) -> bool:
    """
    Run a subprocess step. Returns True on success, False on failure.
    Streams output live to console.
    """
    console.rule(f"[bold cyan]Stage: {name}")
    console.print(f"[dim]$ {' '.join(cmd)}\n")
    start = time.time()

    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=False,   # let output stream to terminal
        text=True,
    )

    elapsed = time.time() - start
    if result.returncode == 0:
        console.print(f"\n[green]✓ {name} — completed in {elapsed:.1f}s")
        return True
    else:
        console.print(f"\n[bold red]✗ {name} — FAILED (exit code {result.returncode}) in {elapsed:.1f}s")
        return False


def main():
    run_start = datetime.now(timezone.utc)
    (PROJECT_ROOT / "gold").mkdir(exist_ok=True)

    venv_scripts = PROJECT_ROOT / ".venv" / ("Scripts" if sys.platform == "win32" else "bin")
    venv_python = venv_scripts / ("python.exe" if sys.platform == "win32" else "python")
    python_bin = str(venv_python) if venv_python.exists() else sys.executable
    dbt_base = [python_bin, "-m", "dbt.cli.main"]

    console.print(Panel(
        f"[bold white]CDC Lakehouse Pipeline\n[dim]{run_start.isoformat()}Z",
        title=">> Pipeline Start",
        border_style="bright_blue"
    ))

    steps: list[tuple[str, list[str], Path | None]] = [
        # (stage_name, command, working_dir or None for project root)
        (
            "Silver Processor (Delta Lake MERGE)",
            [python_bin, "silver_processor.py"],
            None,
        ),
        (
            "Data Quality Gate (Great Expectations)",
            [python_bin, "data_quality/run_checks.py"],
            None,
        ),
        (
            "dbt — Staging models",
            dbt_base + ["run", "--select", "staging.*",
                        "--profiles-dir", ".", "--project-dir", "."],
            PROJECT_ROOT / "dbt_project",
        ),
        (
            "dbt — Mart models",
            dbt_base + ["run", "--select", "marts.*",
                        "--profiles-dir", ".", "--project-dir", "."],
            PROJECT_ROOT / "dbt_project",
        ),
        (
            "dbt — Schema contract tests",
            dbt_base + ["test",
                        "--profiles-dir", ".", "--project-dir", "."],
            PROJECT_ROOT / "dbt_project",
        ),
    ]

    results = []

    for name, cmd, cwd in steps:
        wd = cwd if cwd else PROJECT_ROOT
        success = run_step(name, cmd, wd)
        results.append((name, success))

        if not success:
            console.print(Panel(
                f"[bold red]Pipeline halted at stage:\n  {name}\n\n"
                "Fix the issue above and re-run.",
                title="[X] Pipeline Failed",
                border_style="red"
            ))
            _print_summary(results, run_start)
            sys.exit(1)

    _print_summary(results, run_start)
    console.print(Panel(
        "[bold green]All stages completed successfully!\n"
        "[dim]Launch dashboard:  streamlit run dashboard/app.py",
        title="[OK] Pipeline Complete",
        border_style="green"
    ))


def _print_summary(results: list[tuple[str, bool]], start: datetime):
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    rt = Table(title="Pipeline Summary", show_lines=True)
    rt.add_column("Stage",  style="dim")
    rt.add_column("Result", justify="center")

    for name, success in results:
        status = "[green]✓ PASSED" if success else "[red]✗ FAILED"
        rt.add_row(name, status)

    console.print()
    console.print(rt)
    console.print(f"[dim]Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
