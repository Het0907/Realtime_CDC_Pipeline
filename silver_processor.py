"""
silver_processor.py
====================
Reads Spark Delta Bronze events when available, with JSON consumer output as
a fallback, and writes them into Delta Lake tables in the silver/ directory.

Compatible with deltalake >= 1.0 (delta-rs).

Applies CDC upserts and soft deletes with event ordering and additive schema
evolution. Set CDC_BRONZE_SOURCE to spark or json to select a specific input.

Usage:
    pip install -r requirements.txt
    python silver_processor.py
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from pandas.api.types import is_object_dtype
from rich.console import Console
from rich.table import Table as RichTable

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
BRONZE_DIR = PROJECT_ROOT / "bronze_layer"
SPARK_BRONZE_DIR = PROJECT_ROOT / "spark_bronze/events"
SILVER_DIR = PROJECT_ROOT / "silver"
TABLES = ["customers", "orders", "products"]
ORDER_COLUMNS = ["_event_ts", "_kafka_partition", "_kafka_offset"]

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
        with f.open(encoding="utf-8") as fp:
            event = json.load(fp)
        # Older JSON records stored Kafka coordinates only in the filename.
        parts = f.stem.rsplit("_", 2)
        if len(parts) == 3 and parts[-2].isdigit() and parts[-1].isdigit():
            event.setdefault("partition", int(parts[-2]))
            event.setdefault("offset", int(parts[-1]))
        event["location"] = f.name
        events.append(event)
    return events


def load_spark_bronze_events(table: str) -> list[dict]:
    """Load CDC events from the Spark Structured Streaming Delta Bronze table."""
    if table not in TABLES:
        raise ValueError(f"Unknown CDC table: {table}")
    if not DeltaTable.is_deltatable(str(SPARK_BRONZE_DIR)):
        return []

    bronze = DeltaTable(str(SPARK_BRONZE_DIR))
    columns = ("topic", "partition", "offset", "operation", "event_ts",
               "before_json", "after_json", "ingested_at")
    projection = ", ".join(f'"{column}"' for column in columns)
    records = QueryBuilder().register("bronze", bronze).execute(
        f"""SELECT {projection} FROM bronze WHERE "table" = '{table}'"""
    ).read_all()
    rows = zip(*(records.column(name).to_pylist() for name in columns), strict=True)
    events = []

    for topic, partition, offset, op, event_ts, before, after, ingested_at in rows:
        events.append({
            "table": table,
            "operation": op,
            "timestamp_ms": event_ts or 0,
            "before": json.loads(before) if before is not None else None,
            "after": json.loads(after) if after is not None else None,
            "ingested_at": str(ingested_at) if ingested_at is not None else "",
            "partition": partition,
            "offset": offset,
            "location": f"{topic}:{partition}:{offset}",
        })

    return events


def load_bronze_events(table: str, source: str | None = None) -> list[dict]:
    source = source or bronze_source()
    return load_spark_bronze_events(table) if source == "spark" else load_json_bronze_events(table)


def bronze_source() -> str:
    """Select one Bronze source for all tables in a run."""
    source = os.environ.get("CDC_BRONZE_SOURCE", "auto").lower()
    if source in {"json", "spark"}:
        return source
    if source != "auto":
        raise ValueError("CDC_BRONZE_SOURCE must be auto, spark, or json")
    return "spark" if DeltaTable.is_deltatable(str(SPARK_BRONZE_DIR)) else "json"


def build_silver_row(event: dict) -> dict | None:
    """
    Transform a CDC event into a flat Silver row.
    Returns None for tombstone records we can't process.
    """
    op = event.get("operation", "unknown")
    # For deletes, 'after' is null — use 'before' to know which id to remove
    row_data = event.get("before") if op == "d" else event.get("after")
    if row_data is None:
        return None

    row = dict(row_data)  # copy so we don't mutate

    # Add CDC metadata columns
    row["_cdc_op"]       = op                              # c/u/d/r
    row["_event_ts"]     = int(event.get("timestamp_ms") or 0)
    row["_kafka_partition"] = event.get("partition", -1)
    row["_kafka_offset"] = event.get("offset", -1)
    row["_ingested_at"]  = str(event.get("ingested_at", ""))  # when consumer.py wrote it
    row["_processed_at"] = datetime.now(timezone.utc).isoformat()
    row["_is_deleted"]   = (op == "d")

    return row


def events_to_dataframe(events: list[dict], table: str) -> pd.DataFrame:
    """
    Convert CDC events to a pandas DataFrame.
    Keeps the latest event per primary key using timestamp and Kafka coordinates.
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
    if pk not in df.columns or df[pk].isna().any():
        raise ValueError(f"CDC event for {table} is missing its primary key")
    df = (
        df.sort_values(ORDER_COLUMNS, na_position="first")
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
    for col, dtype in df.dtypes.items():
        if is_object_dtype(dtype):
            df[col] = df[col].astype("string")

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

    for col in ORDER_COLUMNS:
        df[col] = df[col].fillna(0 if col == "_event_ts" else -1).astype("int64")
    if df["id"].isna().any():
        raise ValueError(f"CDC event for {table} has an invalid primary key")

    return df


def silver_table_exists(table: str) -> bool:
    """Check if a Delta table exists at the silver path."""
    silver_path = str(SILVER_DIR / table)
    return DeltaTable.is_deltatable(silver_path)


def write_to_silver(df: pd.DataFrame, table: str) -> None:
    """Merge newer rows into Silver, or initialize the table on first write."""
    silver_path = str(SILVER_DIR / table)
    pk = PRIMARY_KEYS[table]
    df = coerce_dtypes(df.copy(), table)

    if silver_table_exists(table):
        console.log(f"[cyan]Merging {len(df)} rows into existing Silver table: {table}")
        dt = DeltaTable(silver_path)
        target_columns = {field.name for field in dt.schema().fields}
        newer = "FALSE"
        for column in reversed(ORDER_COLUMNS):
            fallback = 0 if column == "_event_ts" else -1
            target = f"COALESCE(target.{column}, {fallback})" if column in target_columns else str(fallback)
            newer = f"(source.{column} > {target} OR (source.{column} = {target} AND {newer}))"

        (
            dt.merge(
                source=df,
                predicate=f"target.{pk} = source.{pk}",
                source_alias="source",
                target_alias="target",
                merge_schema=True,
            )
            .when_matched_update_all(predicate=newer)
            .when_not_matched_insert_all()
            .execute()
        )

    else:
        console.log(f"[yellow]Creating new Silver table: {table}")
        os.makedirs(silver_path, exist_ok=True)
        write_deltalake(
            silver_path,
            df,
            mode="error",
        )


def print_silver_summary(table: str) -> None:
    """Print a summary of the Silver Delta table."""
    silver_path = str(SILVER_DIR / table)
    try:
        dt = DeltaTable(silver_path)
        df = dt.to_pandas()

        rt = RichTable(title=f"Silver: {table}", show_header=True, header_style="bold magenta")
        rt.add_column("Metric",          style="dim")
        rt.add_column("Value")
        rt.add_row("Delta version",      str(dt.version()))
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
    source = bronze_source()
    if source == "spark" and not DeltaTable.is_deltatable(str(SPARK_BRONZE_DIR)):
        raise FileNotFoundError(f"Spark Bronze Delta table is missing: {SPARK_BRONZE_DIR}")

    for table in TABLES:
        console.rule(f"[bold blue]Processing: {table}")

        events = load_bronze_events(table, source)
        if not events:
            console.log(f"[yellow]No bronze events found for {table}, skipping.")
            continue

        console.log(f"Loaded [bold]{len(events)}[/] events from {source} Bronze ({table})")

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
