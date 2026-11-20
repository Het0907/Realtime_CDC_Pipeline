"""
data_quality/gx_setup.py
=========================
Bootstraps a Great Expectations context and registers expectation suites
for each Silver layer table. Run this ONCE to initialise GX.

Usage:
    python data_quality/gx_setup.py
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
GX_DIR       = PROJECT_ROOT / "data_quality" / "gx"

import great_expectations as gx
from run_checks import build_expectations


def get_context():
    """Return a file-backed GX context rooted at data_quality/gx/."""
    GX_DIR.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(
        mode="file",
        project_root_dir=str(GX_DIR),
    )
    return context


def create_suite(context, table: str):
    """Persist the modern GX suite for a Silver table."""
    suite = gx.ExpectationSuite(name=f"{table}_suite")
    for _, expectation in build_expectations(table):
        suite.add_expectation(expectation)
    context.suites.add_or_update(suite)
    print(f"  [ok]   Suite saved: {suite.name}")


def main():
    print("\n── Great Expectations Setup ─────────────────────────────────────────")
    context = get_context()

    for table in ["customers", "orders", "products"]:
        print(f"\n[{table}]")
        create_suite(context, table)

    print("\n✓ GX context initialised at:", GX_DIR)
    print("  Now run:  python data_quality/run_checks.py")


if __name__ == "__main__":
    main()
