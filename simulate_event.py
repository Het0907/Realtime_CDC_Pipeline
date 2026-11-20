"""
simulate_event.py
=================
Interactive demonstration script for presenting real-time CDC updates.

What it does:
  1. Inserts or updates a live record in PostgreSQL (inventory.customers).
  2. Debezium captures the WAL change and publishes to Kafka.
  3. Drains the event into the Bronze layer (consumer.py --batch).
  4. Runs the end-to-end pipeline (Delta Lake MERGE -> GX -> dbt).
  5. Tells you to check your Streamlit dashboard at http://localhost:8501!

Usage:
  python simulate_event.py --insert
  python simulate_event.py --update
"""

import argparse
import random
import subprocess
import sys
import time
from pathlib import Path

import io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

PROJECT_ROOT = Path(__file__).parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / ("Scripts" if sys.platform == "win32" else "bin") / ("python.exe" if sys.platform == "win32" else "python")
PYTHON_BIN = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

FIRST_NAMES = ["Jordan", "Taylor", "Morgan", "Casey", "Riley", "Avery", "Sam", "Dakota"]
LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis"]

def run_cmd(cmd: list[str]) -> bool:
    print(f"\n[RUN] {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return res.returncode == 0

def insert_live_customer():
    first = 'Rajesh'#random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    rand_id = random.randint(1000, 99999)
    email = f"{first.lower()}.{last.lower()}_{rand_id}@demo.com"

    sql = f"INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('{first}', '{last}', '{email}');"
    print(f"\n[1/4] Inserting new customer into Postgres: {first} {last} ({email})...")
    
    docker_cmd = ["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "inventory", "-c", sql]
    if not run_cmd(docker_cmd):
        print("Failed to insert into Postgres via Docker. Is docker-compose running?")
        sys.exit(1)

def update_live_customer():
    new_name = f"VIP_{random.choice(FIRST_NAMES)}"
    sql = f"UPDATE inventory.customers SET first_name = '{new_name}' WHERE id = 1001;"
    print(f"\n[1/4] Updating customer ID 1001 in Postgres: first_name -> '{new_name}'...")
    
    docker_cmd = ["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "inventory", "-c", sql]
    if not run_cmd(docker_cmd):
        print("Failed to update Postgres via Docker. Is docker-compose running?")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Simulate real-time CDC updates for live demo")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--insert", action="store_true", help="Insert a new random customer")
    group.add_argument("--update", action="store_true", help="Update customer 1001's name")
    args = parser.parse_args()

    print("=" * 70)
    print("  >> REAL-TIME CDC LAKEHOUSE LIVE EVENT DEMONSTRATION")
    print("=" * 70)

    # 1. Execute DB mutation
    if args.update:
        update_live_customer()
    else:
        insert_live_customer()

    # 2. Wait 1 second for Debezium to stream from WAL to Kafka
    print("\n[2/4] Debezium streaming WAL event to Kafka topic...")
    time.sleep(1.2)

    # 3. Drain Kafka event to Bronze layer
    print("\n[3/4] Draining Kafka CDC events into Bronze layer (consumer.py)...")
    run_cmd([PYTHON_BIN, "consumer.py", "--batch", "--timeout", "3000"])

    # 4. Trigger Lakehouse pipeline
    print("\n[4/4] Executing Lakehouse Pipeline (Delta Lake MERGE -> GX Gate -> dbt Gold)...")
    run_cmd([PYTHON_BIN, "pipeline/run_pipeline.py"])

    print("\n" + "=" * 70)
    print("  [DONE] DEMO UPDATE COMPLETE!")
    print("  --> Switch to your browser (http://localhost:8501)")
    print("  --> Click 'Rerun' (or press 'R') to see the live update in the Dashboard!")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
