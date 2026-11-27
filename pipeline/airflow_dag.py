"""
pipeline/airflow_dag.py
========================
Equivalent Airflow DAG for the CDC Lakehouse pipeline.

This mirrors the stages in run_pipeline.py using Airflow operators.
Swap in Airflow instead of run_pipeline.py by copying this to your
Airflow dags/ folder.

Requirements (on the Airflow server — NOT needed locally):
    pip install apache-airflow apache-airflow-providers-standard

NOTE: The "Cannot find module 'airflow'" IDE warnings are EXPECTED when
      developing locally. Apache Airflow only runs on the Airflow scheduler/
      worker, not in your local virtual-env.  The runtime guard below
      suppresses import errors on a plain developer machine while keeping the
      file fully functional when deployed to Airflow.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

# ── TYPE_CHECKING guard: resolved only by type-checkers, not at runtime.
#    This eliminates the 4 IDE "missing-import" warnings when apache-airflow
#    is not installed locally.
if TYPE_CHECKING:                                          # pragma: no cover
    from airflow import DAG  # type: ignore
    from airflow.operators.bash import BashOperator  # type: ignore
    from airflow.operators.python import BranchPythonOperator  # type: ignore


def _airflow_available() -> bool:
    """Return True when apache-airflow is importable (i.e. on the Airflow server)."""
    try:
        importlib.import_module("airflow")
        return True
    except ModuleNotFoundError:
        return False


if _airflow_available():
    from airflow import DAG  # type: ignore
    from airflow.operators.bash import BashOperator  # type: ignore

    PROJECT_ROOT = "/opt/airflow/project"   # adjust to your mounted volume path

    default_args = {
        "owner":            "data-engineering",
        "depends_on_past":  False,
        "start_date":       datetime(2026, 1, 1),
        "retries":          1,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": False,
    }

    with DAG(
        dag_id="cdc_lakehouse_pipeline",
        default_args=default_args,
        description="CDC → Bronze → Silver (Delta) → GX Quality Gate → dbt Gold",
        schedule="*/15 * * * *",            # every 15 minutes — near-real-time
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        tags=["cdc", "lakehouse", "delta-lake", "dbt", "great-expectations"],
    ) as dag:

        # ── Stage 1: Silver Processor ─────────────────────────────────────────
        silver_processor = BashOperator(
            task_id="silver_processor",
            bash_command=f"cd {PROJECT_ROOT} && python silver_processor.py",
        )

        # ── Stage 2: Data Quality Gate ────────────────────────────────────────
        data_quality_gate = BashOperator(
            task_id="data_quality_gate",
            bash_command=f"cd {PROJECT_ROOT} && python data_quality/run_checks.py",
            # exit code 1 from run_checks.py will mark this task as failed;
            # downstream dbt tasks will NOT run (default Airflow behaviour)
        )

        # ── Stage 3: dbt Staging ─────────────────────────────────────────────
        dbt_staging = BashOperator(
            task_id="dbt_staging",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_project && mkdir -p ../gold && "
                "dbt run --select staging.* --profiles-dir . --project-dir ."
            ),
        )

        # ── Stage 4: dbt Marts ───────────────────────────────────────────────
        dbt_marts = BashOperator(
            task_id="dbt_marts",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_project && "
                "dbt run --select marts.* --profiles-dir . --project-dir ."
            ),
        )

        # ── Stage 5: dbt Tests (contract enforcement) ─────────────────────────
        dbt_tests = BashOperator(
            task_id="dbt_schema_tests",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_project && "
                "dbt test --profiles-dir . --project-dir ."
            ),
        )

        # ── DAG Dependencies ──────────────────────────────────────────────────
        (
            silver_processor
            >> data_quality_gate
            >> dbt_staging
            >> dbt_marts
            >> dbt_tests
        )

else:
    # ── Running locally without Airflow installed ──────────────────────────────
    if __name__ == "__main__":
        print(
            "\n[airflow_dag.py] Apache Airflow is NOT installed in this environment.\n"
            "This file is a deployment artifact -- copy it to your Airflow server's\n"
            "dags/ folder and run it there.\n\n"
            "To install Airflow locally (optional):\n"
            "    pip install apache-airflow apache-airflow-providers-standard\n"
        )
