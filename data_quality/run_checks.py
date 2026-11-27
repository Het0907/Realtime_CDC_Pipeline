"""
data_quality/run_checks.py
===========================
Runs Great Expectations data-quality checks against the Silver Delta tables.

  - Loads each Silver table as a pandas DataFrame
  - Runs an expectation suite per table
  - Prints a coloured summary to the console
  - EXITS WITH CODE 1 if ANY suite fails - this fails the pipeline

Usage:
    python data_quality/run_checks.py

The pipeline runner (pipeline/run_pipeline.py) calls this and checks the
return code - a non-zero exit blocks dbt from running.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd
from great_expectations.expectations.core import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToNotBeNull,
    ExpectTableRowCountToBeBetween,
)
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# -- Path setup -----------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
SILVER_DIR   = PROJECT_ROOT / "silver"
RESULTS_DIR  = PROJECT_ROOT / "data_quality" / "runtime_results"

console = Console()

# -- Great Expectations suites -----------------------------------------------

def build_expectations(table: str) -> list[tuple[str, object]]:
    """Build business rules for active Silver rows."""
    expectations = []

    if table == "customers":
        expectations.extend([
            ("email not null", ExpectColumnValuesToNotBeNull(column="email")),
            ("email contains @", ExpectColumnValuesToMatchRegex(column="email", regex=r"@")),
            ("first_name not null", ExpectColumnValuesToNotBeNull(column="first_name")),
        ])
    elif table == "orders":
        expectations.extend([
            ("quantity not null", ExpectColumnValuesToNotBeNull(column="quantity")),
            ("quantity > 0", ExpectColumnValuesToBeBetween(
                column="quantity", min_value=0, strict_min=True
            )),
            ("purchaser not null", ExpectColumnValuesToNotBeNull(column="purchaser")),
            ("product_id not null", ExpectColumnValuesToNotBeNull(column="product_id")),
        ])
    elif table == "products":
        expectations.extend([
            ("weight not null", ExpectColumnValuesToNotBeNull(column="weight")),
            ("name not null", ExpectColumnValuesToNotBeNull(column="name")),
            ("weight > 0", ExpectColumnValuesToBeBetween(
                column="weight", min_value=0, strict_min=True
            )),
            ("description not null", ExpectColumnValuesToNotBeNull(column="description")),
        ])

    return expectations


def validate_with_gx(table: str, df: pd.DataFrame) -> list[tuple[str, bool, str]]:
    """Check row identity across Silver and business fields on active rows."""
    checks = evaluate_expectations(table, df, [
        ("Row count > 0", ExpectTableRowCountToBeBetween(min_value=1)),
        ("id not null", ExpectColumnValuesToNotBeNull(column="id")),
        ("id is unique", ExpectColumnValuesToBeUnique(column="id")),
        ("_is_deleted not null", ExpectColumnValuesToNotBeNull(column="_is_deleted")),
        ("_cdc_op in valid set", ExpectColumnValuesToBeInSet(
            column="_cdc_op", value_set=["c", "u", "d", "r"],
        )),
    ])
    if "_is_deleted" in df.columns:
        active = df.loc[df["_is_deleted"] == False]  # noqa: E712
        if not active.empty:
            checks.extend(evaluate_expectations(table, active, build_expectations(table)))
    return checks


def evaluate_expectations(
    table: str, df: pd.DataFrame, expectations: list[tuple[str, object]]
) -> list[tuple[str, bool, str]]:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name=f"{table}_source")
    asset = datasource.add_dataframe_asset(name=table)
    batch_definition = asset.add_batch_definition_whole_dataframe("current")
    batch = batch_definition.get_batch({"dataframe": df})

    suite = gx.ExpectationSuite(name=f"{table}_suite")
    for _, expectation in expectations:
        suite.add_expectation(expectation)

    validator = context.get_validator(batch=batch, expectation_suite=suite)
    validation = validator.validate()
    if len(validation.results) != len(expectations):
        raise RuntimeError(f"Great Expectations returned an incomplete result for {table}")
    checks = []
    for (label, _), result in zip(expectations, validation.results):
        details = result.result or {}
        if "unexpected_count" in details:
            detail = f"Unexpected values: {details['unexpected_count']}"
        elif "observed_value" in details:
            detail = f"Observed: {details['observed_value']}"
        else:
            detail = "GX expectation evaluated"
        if result.exception_info and result.exception_info.get("raised_exception"):
            detail = result.exception_info.get("exception_message", detail)
        checks.append((label, bool(result.success), detail))
    return checks


# -- Main -----------------------------------------------------------------------

def load_silver_table(table: str) -> pd.DataFrame:
    """Load a Silver Delta table as pandas using deltalake."""
    from deltalake import DeltaTable
    silver_path = str(SILVER_DIR / table)
    try:
        dt = DeltaTable(silver_path)
        return dt.to_pandas()
    except Exception as e:
        console.log(f"[red]Failed to load silver/{table}: {e}")
        return pd.DataFrame()


def save_results(all_results: dict) -> Path:
    """Save JSON results for the dashboard to read."""
    import json
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = RESULTS_DIR / f"gx_results_{ts}.json"
    # Also write latest (overwrite)
    latest = RESULTS_DIR / "gx_results_latest.json"

    payload = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "tables": {}
    }
    for table, checks in all_results.items():
        payload["tables"][table] = {
            "passed": sum(1 for _, p, _ in checks if p),
            "failed": sum(1 for _, p, _ in checks if not p),
            "checks": [
                {"name": n, "passed": p, "detail": d}
                for n, p, d in checks
            ]
        }
    for target in [path, latest]:
        with open(target, "w") as f:
            json.dump(payload, f, indent=2)
    return latest


def main() -> int:
    console.rule("[bold cyan]Data Quality Gate - Great Expectations")
    console.print(f"[dim]Run time: {datetime.now(timezone.utc).isoformat()}Z\n")

    all_results   = {}
    overall_pass  = True

    for table in ["customers", "orders", "products"]:
        df = load_silver_table(table)

        if df.empty:
            console.print(f"[bold red]-  {table}: No data found in Silver table - FAIL")
            all_results[table] = [("Data available", False, "Silver table is empty or missing")]
            overall_pass = False
            continue

        checks = validate_with_gx(table, df)
        all_results[table] = checks

        # Build rich table
        rt = Table(title=f"[bold]{table}[/]", show_lines=True)
        rt.add_column("Check",  style="dim",   min_width=30)
        rt.add_column("Status", justify="center", min_width=8)
        rt.add_column("Detail", style="dim",   min_width=30)

        table_pass = True
        for name, passed, detail in checks:
            status = "[bold green]- PASS" if passed else "[bold red]- FAIL"
            rt.add_row(name, status, detail)
            if not passed:
                table_pass = False
                overall_pass = False

        console.print(rt)
        console.print()

    # Save results for dashboard
    results_path = save_results(all_results)
    console.print(f"[dim]Results saved: {results_path}")

    # Final verdict
    if overall_pass:
        console.print(Panel(
            "[bold green]- ALL DATA QUALITY CHECKS PASSED\nPipeline may continue.",
            title="Quality Gate", border_style="green"
        ))
        return 0
    else:
        console.print(Panel(
            "[bold red]- DATA QUALITY VIOLATIONS DETECTED\nPipeline HALTED. Fix issues before proceeding.",
            title="Quality Gate", border_style="red"
        ))
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
