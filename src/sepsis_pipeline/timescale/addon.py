"""Stage 8 — TimescaleDB add-on: hypertable, columnstore, continuous aggregate, gap-filling
cross-check and an honest benchmark against the plain gold table (docs/timescale_benchmark.md).

Isolated: everything lives in schema ts and nothing in Stages 0-7 depends on it.
"""

import statistics
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql

from sepsis_pipeline.config import PROJECT_ROOT, Settings
from sepsis_pipeline.db import connect
from sepsis_pipeline.leakage import LEAKAGE_TEST_PATIENTS

SQL_DIR = PROJECT_ROOT / "sql" / "timescale"
RUNS = 5

# (id, description, SQL template). {t} = table, {time} = its time column.
QUERIES = [
    ("Q1", "One patient's full trajectory",
     "SELECT * FROM {t} WHERE patient_key = %(patient)s ORDER BY {time}"),
    ("Q2", "Early window: per-variable average, stay hours 1-24",
     "SELECT variable_key, avg(value) FROM {t} WHERE {time} BETWEEN 1 AND 24 GROUP BY 1"),
    ("Q3", "Late window: per-variable average, stay hours 168-336",
     "SELECT variable_key, avg(value) FROM {t} WHERE {time} BETWEEN 168 AND 336 GROUP BY 1"),
    ("Q4", "Average HR per 6-hour bucket over all patients",
     "SELECT ({time} - 1) / 6 AS bucket, avg(value) FROM {t}"
     " WHERE variable_key = %(hr)s AND NOT is_out_of_range GROUP BY 1"),
    ("Q5", "One variable (lactate) across all patients",
     "SELECT count(*), avg(value) FROM {t} WHERE variable_key = %(lactate)s"),
]  # fmt: skip
CAGG_Q4 = (
    "SELECT bucket, sum(avg_value * n) / sum(n) FROM ts.cagg_patient_6h"
    " WHERE variable_key = %(hr)s GROUP BY bucket"
)
TARGETS = {
    "plain": ("gold.fact_measurement", "stay_hour_key"),
    "hypertable": ("ts.measurement", "iculos"),
}


@dataclass
class TimescaleReport:
    generated_at: datetime
    sizes: dict[str, int] = field(default_factory=dict)
    chunks: list[tuple] = field(default_factory=list)
    timings: dict[tuple[str, str], float] = field(default_factory=dict)
    compression: list[tuple] = field(default_factory=list)
    gapfill_rows_compared: int = 0
    gapfill_mismatches: int = 0
    gapfill_beyond_cap: int = 0
    notes: list[str] = field(default_factory=list)


STATEMENT_BREAK = "-- @@ statement break"


def _run_script(conn: psycopg.Connection, name: str) -> None:
    """Run a script; parts separated by STATEMENT_BREAK are sent separately, because Postgres
    runs a multi-statement string as one implicit transaction, which some TimescaleDB
    procedures (e.g. refresh_continuous_aggregate) refuse."""
    for part in split_script((SQL_DIR / name).read_text(encoding="utf-8")):
        conn.execute(part)


def split_script(text: str) -> list[str]:
    """Split on whole lines starting with STATEMENT_BREAK (the marker line itself is dropped)."""
    parts, current = [], []
    for line in text.splitlines():
        if line.strip().startswith(STATEMENT_BREAK):
            parts.append("\n".join(current))
            current = []
        else:
            current.append(line)
    parts.append("\n".join(current))
    return [p for p in parts if p.strip()]


def _median_ms(conn: psycopg.Connection, query: str, params: dict) -> float:
    times = []
    for _ in range(RUNS):
        plan = conn.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query, params).fetchone()[
            0
        ]
        times.append(plan[0]["Execution Time"])
    return statistics.median(times)


def _benchmark(
    conn, report: TimescaleReport, variant: str, table: str, time_col: str, params
) -> None:
    for qid, _, template in QUERIES:
        query = template.format(t=table, time=time_col)
        report.timings[(qid, variant)] = _median_ms(conn, query, params)


GAPFILL_VARIABLES = ("hr", "temp", "lactate")  # dense, every few hours, sparse


def _gapfill_cross_check(conn: psycopg.Connection, report: TimescaleReport) -> None:
    """time_bucket_gapfill + locf per patient vs dbt's capped LOCF (int_patient_hour_imputed).

    Three variables with very different measurement frequency, so both branches are exercised:
    within the cap the values must match; beyond it dbt returns NULL while locf() carries on.
    """
    for variable in GAPFILL_VARIABLES:
        variable_key, cap = conn.execute(
            "SELECT d.variable_key, c.locf_max_hours FROM gold.dim_variable d"
            " JOIN silver.variable_catalog c USING (variable) WHERE variable = %s",
            (variable,),
        ).fetchone()
        for patient_id in LEAKAGE_TEST_PATIENTS:
            patient_key, first, last = conn.execute(
                "SELECT p.patient_key, s.admit_stay_hour_key, s.last_stay_hour_key"
                " FROM gold.dim_patient p JOIN gold.fact_icu_stay s USING (patient_key)"
                " WHERE p.patient_id = %s",
                (patient_id,),
            ).fetchone()
            gapfilled = dict(
                conn.execute(
                    "SELECT time_bucket_gapfill(1, iculos, %(start)s, %(finish)s) AS hour,"
                    "       locf(last(value, iculos))"
                    " FROM ts.measurement"
                    " WHERE patient_key = %(p)s AND variable_key = %(v)s"
                    "   AND NOT is_out_of_range AND NOT is_unit_mismatch"
                    "   AND iculos BETWEEN %(start)s AND %(last)s"
                    " GROUP BY hour ORDER BY hour",
                    {"start": first, "finish": last + 1, "last": last, "p": patient_key,
                     "v": variable_key},
                ).fetchall()
            )  # fmt: skip
            rows = conn.execute(
                sql.SQL(
                    "SELECT iculos, {}, {} FROM silver.int_patient_hour_imputed"
                    " WHERE patient_id = %s ORDER BY iculos"
                ).format(
                    sql.Identifier(f"{variable}_last"), sql.Identifier(f"{variable}_hours_since")
                ),
                (patient_id,),
            ).fetchall()
            for hour, dbt_last, hours_since in rows:
                ts_value = gapfilled.get(hour)
                report.gapfill_rows_compared += 1
                if hours_since is not None and hours_since <= cap:
                    if ts_value is None or abs(ts_value - dbt_last) > 1e-9:
                        report.gapfill_mismatches += 1
                elif hours_since is not None:  # beyond the cap: dbt NULL, gapfill still carries
                    report.gapfill_beyond_cap += 1
                    if dbt_last is not None or ts_value is None:
                        report.gapfill_mismatches += 1
                elif ts_value is not None or dbt_last is not None:  # never observed: both NULL
                    report.gapfill_mismatches += 1


def _exists(conn: psycopg.Connection, schema: str, name: str) -> bool:
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{schema}.{name}",)).fetchone()[0]


def _compressed_chunks(conn: psycopg.Connection, hypertable: str) -> int:
    row = conn.execute(
        "SELECT number_compressed_chunks FROM hypertable_columnstore_stats(%s)", (hypertable,)
    ).fetchone()
    return (row[0] or 0) if row else 0


def _convert_to_columnstore(conn: psycopg.Connection) -> None:
    """Enable the columnstore (02_columnstore.sql) and convert chunk by chunk, one transaction
    each, skipping converted chunks: an interrupted run loses at most one chunk of work."""
    enabled = conn.execute(
        "SELECT compression_enabled FROM timescaledb_information.hypertables"
        " WHERE hypertable_schema = 'ts' AND hypertable_name = 'measurement'"
    ).fetchone()[0]
    if not enabled:
        _run_script(conn, "02_columnstore.sql")
    for (chunk,) in conn.execute("SELECT show_chunks('ts.measurement')::text").fetchall():
        conn.execute(
            "CALL convert_to_columnstore(%s::regclass, if_not_columnstore => true)", (chunk,)
        )
    conn.execute("ANALYZE ts.measurement")


def run_timescale(settings: Settings, resume: bool = False) -> TimescaleReport:
    report = TimescaleReport(generated_at=datetime.now(UTC))
    with connect(settings, autocommit=True) as conn:
        keys = dict(
            conn.execute(
                "SELECT variable, variable_key FROM gold.dim_variable"
                " WHERE variable IN ('hr', 'lactate')"
            ).fetchall()
        )
        patient = conn.execute(
            "SELECT patient_key FROM gold.dim_patient WHERE patient_id = %s",
            (LEAKAGE_TEST_PATIENTS[0],),
        ).fetchone()[0]
        params = {"patient": patient, "hr": keys["hr"], "lactate": keys["lactate"]}

        if not resume or not _exists(conn, "ts", "measurement"):
            _run_script(conn, "01_hypertable.sql")
        report.sizes["gold.fact_measurement (plain table)"] = conn.execute(
            "SELECT pg_total_relation_size('gold.fact_measurement')"
        ).fetchone()[0]
        report.chunks = conn.execute(
            "SELECT time_bucket(24, iculos) AS chunk_start, count(*) AS n_rows"
            " FROM ts.measurement GROUP BY 1 ORDER BY 1"
        ).fetchall()
        _benchmark(conn, report, "plain table", *TARGETS["plain"], params)
        if _compressed_chunks(conn, "ts.measurement") == 0:
            report.sizes["ts.measurement (hypertable, rowstore)"] = conn.execute(
                "SELECT total_bytes FROM hypertable_detailed_size('ts.measurement')"
            ).fetchone()[0]
            _benchmark(conn, report, "hypertable (rowstore)", *TARGETS["hypertable"], params)
        else:  # resumed after conversion: the rowstore no longer exists to be timed
            report.notes.append(
                "Resumed run: the hypertable was already (partly) converted, so rowstore timings"
                " are not available; its size is the pre-compression size."
            )

        _convert_to_columnstore(conn)
        report.compression = conn.execute(
            "SELECT total_chunks, number_compressed_chunks, before_compression_total_bytes,"
            " after_compression_total_bytes FROM hypertable_columnstore_stats('ts.measurement')"
        ).fetchall()
        report.sizes.setdefault(
            "ts.measurement (hypertable, rowstore)", report.compression[0][2] or 0
        )
        report.sizes["ts.measurement (columnstore, segmentby variable)"] = conn.execute(
            "SELECT total_bytes FROM hypertable_detailed_size('ts.measurement')"
        ).fetchone()[0]
        _benchmark(conn, report, "hypertable (columnstore)", *TARGETS["hypertable"], params)

        if (
            not resume
            or not _exists(conn, "ts", "measurement_by_patient")
            or _compressed_chunks(conn, "ts.measurement_by_patient") == 0
        ):
            _run_script(conn, "03_segmentby_patient.sql")
        report.sizes["ts.measurement_by_patient (columnstore, segmentby patient)"] = conn.execute(
            "SELECT total_bytes FROM hypertable_detailed_size('ts.measurement_by_patient')"
        ).fetchone()[0]

        if not resume or not _exists(conn, "ts", "cagg_patient_6h"):
            _run_script(conn, "04_continuous_aggregate.sql")
        report.timings[("Q4", "continuous aggregate")] = _median_ms(conn, CAGG_Q4, params)

        _gapfill_cross_check(conn, report)
    return report


def _mb(n: int) -> str:
    return f"{n / 2**20:,.1f} MB"


def render_report(report: TimescaleReport) -> str:
    variants = ["plain table", "hypertable (rowstore)", "hypertable (columnstore)"]
    total_rows = sum(n for _, n in report.chunks)
    lines = [
        "# TimescaleDB benchmark",
        "",
        f"_Generated by `sepsis timescale` on {report.generated_at:%Y-%m-%d %H:%M} UTC. "
        f"Each timing is the median of {RUNS} runs of `EXPLAIN (ANALYZE, BUFFERS)` "
        "(server-side execution time, warm cache)._",
        "",
        "Data: `gold.fact_measurement` (one row per observation) copied into the hypertable "
        "`ts.measurement`, with integer time = hours since ICU admission and 24-hour chunks.",
        "",
        *[f"> {n}" for n in report.notes],
        *([""] if report.notes else []),
        "## 1. Storage",
        "",
        "| Relation | Size |",
        "|---|---|",
        *[f"| {k} | {_mb(v)} |" for k, v in report.sizes.items()],
        "",
    ]
    if report.compression:
        total, compressed, before, after = report.compression[0]
        lines += [
            f"Columnstore: {compressed}/{total} chunks converted; "
            f"{_mb(before or 0)} before, {_mb(after or 0)} after "
            f"(**{(before or 1) / max(after or 1, 1):.1f}x** smaller).",
            "",
        ]
    lines += [
        "## 2. Chunk skew (rows per 24-hour chunk)",
        "",
        "| Chunk (hours of stay) | Rows | Share |",
        "|---|---|---|",
        *[f"| {h}-{h + 23} | {n:,} | {n / total_rows:.1%} |" for h, n in report.chunks],
        "",
        "Most rows sit in the first two chunks (most stays are short), so time-range queries on "
        "early hours cannot skip much data.",
        "",
        "## 3. Query timings (ms)",
        "",
        "| Query | " + " | ".join(variants) + " | continuous aggregate |",
        "|---|" + "---|" * (len(variants) + 1),
    ]
    for qid, description, _ in QUERIES:
        cells = [
            f"{report.timings[(qid, v)]:.1f}" if (qid, v) in report.timings else "n/a"
            for v in variants
        ]
        cagg = report.timings.get((qid, "continuous aggregate"))
        cells.append(f"{cagg:.1f}" if cagg is not None else "")
        lines.append(f"| {qid}: {description} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "## 4. Gap-filling cross-check",
        "",
        f"`time_bucket_gapfill(1, iculos) + locf()` for {', '.join(GAPFILL_VARIABLES)} on the "
        f"{len(LEAKAGE_TEST_PATIENTS)} leakage-test patients vs dbt's capped LOCF "
        f"(`int_patient_hour_imputed.<var>_last`): {report.gapfill_rows_compared} "
        f"variable-hours compared, **{report.gapfill_mismatches} mismatches**. "
        f"{report.gapfill_beyond_cap} of them were beyond the variable's LOCF cap; there dbt "
        "returns NULL while the uncapped `locf()` keeps carrying the last value, which is the "
        "expected difference and is checked as such.",
        "",
        "## 5. Interpretation (generated from the measurements above)",
        "",
        *_interpretation(report),
        "",
        "Structural context: the dataset has relative time only, so Timescale's wall-clock "
        "features (retention policies, real-time aggregation of newly arriving data) do not "
        "apply, and a patient's rows span several chunks, so time partitioning cannot help "
        "per-patient access. This is why TimescaleDB is an isolated add-on, not the core of the "
        "warehouse.",
        "",
    ]
    return "\n".join(lines)


def _compare(fast: float, slow: float) -> str:
    if fast <= 0 or slow <= 0:
        return "n/a"
    ratio = slow / fast
    if 0.9 <= ratio <= 1.1:
        return "about the same as"
    return f"{ratio:.1f}x faster than" if ratio > 1 else f"{1 / ratio:.1f}x slower than"


def _interpretation(report: TimescaleReport) -> list[str]:
    bullets = []
    plain = report.sizes.get("gold.fact_measurement (plain table)")
    columnstore = report.sizes.get("ts.measurement (columnstore, segmentby variable)")
    by_patient = report.sizes.get("ts.measurement_by_patient (columnstore, segmentby patient)")
    if plain and columnstore:
        bullets.append(
            f"- Storage: the columnstore hypertable is {_mb(columnstore)} vs {_mb(plain)} for the "
            f"plain table ({plain / columnstore:.1f}x smaller, including indexes)."
        )
    if columnstore and by_patient:
        better = "worse" if by_patient > columnstore else "better"
        bullets.append(
            f"- segmentby patient_key compresses {better} ({_mb(by_patient)}) than segmentby "
            f"variable_key ({_mb(columnstore)})."
        )
    for qid, description, _ in QUERIES:
        p = report.timings[(qid, "plain table")]
        c = report.timings[(qid, "hypertable (columnstore)")]
        r = report.timings.get((qid, "hypertable (rowstore)"))
        rowstore = f"; rowstore hypertable is {_compare(r, p)} the plain table" if r else ""
        bullets.append(
            f"- {qid} ({description}): columnstore is {_compare(c, p)} the plain table{rowstore}."
        )
    cagg = report.timings.get(("Q4", "continuous aggregate"))
    if cagg is not None:
        p = report.timings[("Q4", "plain table")]
        bullets.append(
            f"- Q4 from the continuous aggregate is {_compare(cagg, p)} the plain table."
        )
    return bullets


def write_report(report: TimescaleReport, path: Path | None = None) -> Path:
    path = path or PROJECT_ROOT / "docs" / "timescale_benchmark.md"
    path.write_text(render_report(report), encoding="utf-8")
    return path
