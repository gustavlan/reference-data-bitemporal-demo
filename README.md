# Bi-Temporal Reference Data Pipeline Demo

Builds a minimal reference data pipeline that keeps a full bi-temporal history of every change. The project demonstrates how to:

- Track both event/valid time (when a fact was true) and knowledge/transaction time (when we learned about the fact).
- Apply Slowly Changing Dimension Type 2 (SCD2) rules for idempotent merges into SQLite, keeping history instead of overwriting rows.
- Orchestrate extract → transform → load stages with Apache Airflow (Local Executor) or via a lightweight CLI runner.
- Query “as-of” snapshots to recover what the system believed at any point in time, even after backfills or late-arriving corrections.
- Guard against duplicate processing with an event-time watermark and thorough pytest coverage.


## Architecture

- **Data source** — Synthetic CSV (`data/synthetic_refdata.csv`) with a few securities out of order to illustrate late arrivals.
- **Pipeline orchestration** — `dags/reference_data_pipeline.py` defines three Airflow tasks (extract, transform, load) that run sequentially under the Local Executor.
- **Merge engine** — `bitemporal/merge_logic.py` wraps SQLite transactions to enforce SCD2 semantics, including overlap checks knowledge timeline updates and watermark persistence.
- **Utilities** — A CLI runner (`scripts/run_pipeline.py`) for quick demos and a snapshot query helper (`scripts/query_as_of.py`).
- **Tests** — Pytest suites verify merge behaviour, as-of queries and watermark controls.

## Repository Layout

```
reference-data-bitemporal-demo/
├── bitemporal/              # Merge logic package (initialise DB merge, query)
├── dags/                    # Airflow DAG orchestrating the ETL pipeline
├── data/                    # Synthetic CSV data
├── scripts/                 # CLI helpers (pipeline runner, as-of query)
├── tests/                   # Pytest suites covering core behaviours
├── pyproject.toml           # Project metadata & optional dependencies
└── requirements.txt         # Dev/test dependency pin stub
```

## Getting Started

Requirements:

- Python 3.9+
- Optional: Apache Airflow ≥ 2.9 (install with `pip install -e .[airflow]`)

Setup steps:

```bash
cd reference-data-bitemporal-demo
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
```

This installs pytest and, if requested, Airflow with the Local Executor.

## Running the Pipeline Without Airflow

The quickest way to see the merge logic in action is via the CLI runner:

```bash
cd reference-data-bitemporal-demo
python scripts/run_pipeline.py
```

- Reads `data/synthetic_refdata.csv`
- Applies a consistent knowledge timestamp
- Merges everything into `reference_data.db`

Re-run to observe idempotency, no duplicate rows are produced.

### Controlled Backfills

Add the `--allow-late` flag to process records whose event times are older than the stored watermark:

```bash
python scripts/run_pipeline.py --allow-late
```

This simulates late-arriving corrections and triggers the backfill branch of the merge algorithm.

## Orchestrating with Airflow

1. Install Airflow and configure the Local Executor. A minimal local setup can rely on the default SQLite metadata database.
2. Place `dags/reference_data_pipeline.py` in your Airflow `dags/` folder (or mount this repository).
3. Ensure `reference-data-bitemporal-demo` is available on the Airflow worker `PYTHONPATH` (e.g., by installing it with `pip install -e .`).
4. Start the Airflow scheduler and trigger the `reference_data_ingestion` DAG via the UI or CLI.

The DAG exposes an optional `allow_late` run configuration flag so you can trigger backfills on demand (`airflow dags trigger reference_data_ingestion -c '{"allow_late": true}'`).

## Querying Historical Snapshots

Use the provided helper to inspect the database state as it was known on any knowledge date:

```bash
python scripts/query_as_of.py 2025-03-01T00:00:00Z
```

Add `--effective-time` to answer questions like “as of 2025‑04‑10, what do we
now believe the data was on 2025‑02‑20?”

```bash
python scripts/query_as_of.py 2025-04-10T00:00:00Z --effective-time 2025-02-20T00:00:00Z
```

The underlying SQL filters on both knowledge and valid timelines:

$$
	ext{knowledge\_from} \le t_k < \text{knowledge\_to}\,(\text{or null})\\
	ext{valid\_from} \le t_v < \text{valid\_to}\,(\text{or null})
$$

## Tests & Quality Gates

All critical behaviours ship with pytest coverage. Execute the suite from the project root:

```bash
cd reference-data-bitemporal-demo
python -m pytest
```

The tests cover:
- New inserts vs. updates (including corrections at the same event boundary)
- Backfills that split existing timelines
- As-of knowledge/effective date queries
- Watermark enforcement and deliberate overrides

## Synthetic Data & Schema

The SQLite table `reference_data` stores:

| Column | Description |
| --- | --- |
| `security_id` | Business key of the reference entity |
| `attributes` | JSON blob of attributes (name, status, etc.) |
| `event_time` | Original event timestamp for traceability |
| `valid_from` / `valid_to` | Valid (event) timeline range |
| `knowledge_from` / `knowledge_to` | Knowledge (transaction) timeline range |
| `is_current` | Convenience flag (`1` = still current in both timelines) |

Table `pipeline_watermarks` keeps the latest processed event time to guard against unintended reprocessing.
