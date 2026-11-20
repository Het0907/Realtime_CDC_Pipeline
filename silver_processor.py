"""
silver_processor.py
====================
Reads Spark Delta Bronze events when available, with JSON consumer output as
a fallback, and writes them into Delta Lake tables in the silver/ directory.

Compatible with deltalake >= 1.0 (delta-rs).

Key features:
  - MERGE (upsert) -- exactly-once semantics for inserts and updates
  - Hard-delete handling -- soft-deletes rows on CDC op='d'
  - Late-arriving data -- events are ordered by event timestamp, not file mtime
  - Schema evolution -- new columns handled automatically on initial write
  - Metadata columns -- _cdc_op, _event_ts, _ingested_at preserved on every row

Usage:
    pip install -r requirements.txt
    python silver_processor.py
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from rich.console import Console
from rich.table import Table as RichTable

# ── Config ─────────────────────────────────────────────────────────────────────
BRONZE_DIR = Path("bronze_layer")
SPARK_BRONZE_DIR = Path("spark_bronze/events")
SILVER_DIR = Path("silver")
TABLES = ["customers", "orders", "products"]

# Primary key per table — used for MERGE predicate
PRIMARY_KEYS = {
    "customers": "id",
    "orders":    "id",
    "products":  "id",
}

console = Console()


# ── Helpers ────────────────────────────────────────────────────────────────────

def load_json_bronze_events(table: str) -> list[dict]:
    """Load CDC events written by the fallback JSON consumer."""
    table_dir = BRONZE_DIR / table
    events = []
    if not table_dir.exists():
        return events
    for f in sorted(table_dir.glob("*.json")):
        with open(f) as fp:
            events.append(json.load(fp))
    return events


def load_spark_bronze_events(table: str) -> list[dict]:
    """Load CDC events from the Spark Structured Streaming Delta Bronze table."""
    if not DeltaTable.is_deltatable(str(SPARK_BRONZE_DIR)):
        return []

    frame = DeltaTable(str(SPARK_BRONZE_DIR)).to_pandas()
    frame = frame[frame["table"] == table] if "table" in frame.columns else frame.iloc[0:0]
    events = []

    for _, row in frame.iterrows():
        def decode_json(value):
            if value is None or pd.isna(value):
                return None
            return json.loads(value)

        events.append({
            "table": table,
            "operation": row.get("operation", "unknown"),
            "timestamp_ms": int(row.get("event_ts", 0) or 0),
            "before": decode_json(row.get("before_json")),
            "after": decode_json(row.get("after_json")),
            "ingested_at": str(row.get("ingested_at", "")),
        })

    return events


def load_bronze_events(table: str) -> list[dict]:
    """Load Spark Bronze events, falling back to JSON Bronze when unavailable."""
    events = load_spark_bronze_events(table)
    if not events:
        events = load_json_bronze_events(table)
    # Sort by CDC event timestamp to handle late-arriving data.
    events.sort(key=lambda e: e.get("timestamp_ms", 0))
    return events


def bronze_source() -> str:
    """Return the active Bronze source for status output."""
    if DeltaTable.is_deltatable(str(SPARK_BRONZE_DIR)):
        return "spark_bronze/events"
    return "bronze_layer JSON"


def build_silver_row(event: dict) -> dict | None:
    """
    Transform a CDC event into a flat Silver row.
    Returns None for tombstone records we can't process.
    """
    op = event.get("operation", "unknown")
    # For deletes, 'after' is null — use 'before' to know which id to remove
    row_data = event.get("after") or event.get("before")
    if row_data is None:
        return None

    row = dict(row_data)  # copy so we don't mutate

    # Add CDC metadata columns
    row["_cdc_op"]       = op                              # c/u/d/r
    row["_event_ts"]     = int(event.get("timestamp_ms", 0))  # source event time (ms)
    row["_ingested_at"]  = str(event.get("ingested_at", ""))  # when consumer.py wrote it
    row["_processed_at"] = datetime.now(timezone.utc).isoformat()
    row["_is_deleted"]   = (op == "d")

    return row


def events_to_dataframe(events: list[dict], table: str) -> pd.DataFrame:
    """
    Convert CDC events to a pandas DataFrame.
    Handles late-arriving data by deduplicating on (pk, event_ts) — keeping latest.
    """
    rows = []
    for event in events:
        row = build_silver_row(event)
        if row is not None:
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    pk = PRIMARY_KEYS[table]
    if pk in df.columns:
        # Keep only the latest event per primary key (handles late-arriving duplicates)
        df = (
            df.sort_values("_event_ts", ascending=True)
              .drop_duplicates(subset=[pk], keep="last")
              .reset_index(drop=True)
        )

    # Decode Debezium epoch-day integer -> human-readable date string
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(
            df["order_date"], unit="D", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    return df


def coerce_dtypes(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Force consistent dtypes so pyarrow doesn't choke on mixed-type columns."""
    # Cast all object columns to string
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str)

    # Cast bool column
    if "_is_deleted" in df.columns:
        df["_is_deleted"] = df["_is_deleted"].astype(bool)

    # Cast integer-ish numeric columns
    for col in ["id", "purchaser", "product_id", "quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Cast float-ish columns
    for col in ["weight"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # event_ts should be int64
    if "_event_ts" in df.columns:
        df["_event_ts"] = pd.to_numeric(df["_event_ts"], errors="coerce").fillna(0).astype("int64")

    return df


def silver_table_exists(table: str) -> bool:
    """Check if a Delta table exists at the silver path."""
    silver_path = str(SILVER_DIR / table)
    return DeltaTable.is_deltatable(silver_path)


def write_to_silver(df: pd.DataFrame, table: str) -> None:
    """
    Write/merge DataFrame into a Delta Lake Silver table.
    Uses MERGE for upsert semantics (exactly-once). 
    Falls back to overwrite write on first run.
    """
    silver_path = str(SILVER_DIR / table)
    pk = PRIMARY_KEYS[table]
    df = coerce_dtypes(df.copy(), table)

    if silver_table_exists(table):
        console.log(f"[cyan]Merging {len(df)} rows into existing Silver table: {table}")
        dt = DeltaTable(silver_path)

        (
            dt.merge(
                source=df,
                predicate=f"target.{pk} = source.{pk}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    else:
        console.log(f"[yellow]Creating new Silver table: {table}")
        os.makedirs(silver_path, exist_ok=True)
        write_deltalake(
            silver_path,
            df,
            mode="overwrite",
        )


def print_silver_summary(table: str) -> None:
    """Print a summary of the Silver Delta table."""
    silver_path = str(SILVER_DIR / table)
    try:
        dt = DeltaTable(silver_path)
        history = dt.history()
        df = dt.to_pandas()

        rt = RichTable(title=f"Silver: {table}", show_header=True, header_style="bold magenta")
        rt.add_column("Metric",          style="dim")
        rt.add_column("Value")
        rt.add_row("Delta version",      str(len(history) - 1))
        rt.add_row("Total rows",         str(len(df)))
        rt.add_row("Columns",            str(len(df.columns)))
        rt.add_row("Column names",       ", ".join(df.columns.tolist()))

        if "_is_deleted" in df.columns:
            active  = (df["_is_deleted"] == False).sum()  # noqa: E712
            deleted = (df["_is_deleted"] == True).sum()   # noqa: E712
            rt.add_row("Active rows",    str(active))
            rt.add_row("Soft-deleted",   str(deleted))

        console.print(rt)
    except Exception as e:
        console.log(f"[red]Could not read Silver table {table}: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    console.rule("[bold green]Silver Layer Processor — Delta Lake")
    SILVER_DIR.mkdir(exist_ok=True)

    for table in TABLES:
        console.rule(f"[bold blue]Processing: {table}")

        events = load_bronze_events(table)
        if not events:
            console.log(f"[yellow]No bronze events found for {table}, skipping.")
            continue

        console.log(f"Loaded [bold]{len(events)}[/] events from {bronze_source()} ({table})")

        df = events_to_dataframe(events, table)
        if df.empty:
            console.log(f"[yellow]No valid rows to process for {table}")
            continue

        console.log(f"Built DataFrame with [bold]{len(df)}[/] deduplicated rows")

        write_to_silver(df, table)
        print_silver_summary(table)

    console.rule("[bold green]Silver processing complete [DONE]")
    console.print("\n[dim]Delta table files are in: silver/{customers,orders,products}/")
    console.print("[dim]Time travel: DeltaTable('silver/customers', version=0).to_pandas()")


if __name__ == "__main__":
    main()
