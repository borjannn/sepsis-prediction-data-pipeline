"""Stage 2 — Profile the bronze data (docs/data_profile.md + audit.dq_results).

Structural checks (uniqueness, contiguity, label monotonicity, ...) run on all patients: they
describe the data format and fit nothing. Value distributions that inform cleaning decisions
(ranges, unit harmonisation) are computed on **train-split patients only** (§4.2 of the plan).
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import psycopg

from sepsis_pipeline.bronze.schema import LABEL, LABS, SOURCE_COLUMNS, VITALS

QUANTILES = {
    "p0.1": 0.001,
    "p1": 0.01,
    "p5": 0.05,
    "p50": 0.5,
    "p95": 0.95,
    "p99": 0.99,
    "p99.9": 0.999,
}
STATIC_COLUMNS = ["Age", "Gender", "Unit1", "Unit2", "HospAdmTime"]
ICULOS_BUCKETS = [(1, 24), (25, 48), (49, 72), (73, 168), (169, 336), (337, 10_000)]


@dataclass
class Check:
    name: str
    holds: bool
    observed: str
    metric: float


@dataclass
class Profile:
    generated_at: datetime
    overview: dict[str, int]
    split_table: pl.DataFrame
    checks: list[Check]
    facts: dict[str, str]
    variable_stats: pl.DataFrame
    iculos_buckets: pl.DataFrame
    notes: list[str] = field(default_factory=list)


def _patient_summary(lf: pl.LazyFrame) -> pl.DataFrame:
    """One row per patient; only the columns needed are scanned (memory stays small)."""
    order = ["ICULOS", "source_row_number"]
    label = pl.col(LABEL).sort_by(order)
    return (
        lf.select("patient_id", "source_set", *order, LABEL, *STATIC_COLUMNS)
        .group_by("patient_id")
        .agg(
            source_set=pl.col("source_set").first(),
            rows=pl.len(),
            min_iculos=pl.col("ICULOS").min(),
            max_iculos=pl.col("ICULOS").max(),
            n_unique_iculos=pl.col("ICULOS").n_unique(),
            is_sepsis=pl.col(LABEL).max(),
            label_decreases=(label.diff() < 0).sum(),
            first_positive_row=pl.arg_where(label == 1).first(),
            **{f"{c}_distinct": pl.col(c).drop_nulls().n_unique() for c in STATIC_COLUMNS},
            **{f"{c}_nulls": pl.col(c).null_count() for c in STATIC_COLUMNS},
            **{f"{c}_first": pl.col(c).sort_by(order).first() for c in STATIC_COLUMNS},
        )
        .sort("patient_id")
        .collect()
    )


def _checks(patients: pl.DataFrame) -> list[Check]:
    n = patients.height
    dup_rows = int((patients["rows"] - patients["n_unique_iculos"]).sum())
    gaps = patients.filter(
        pl.col("max_iculos") - pl.col("min_iculos") + 1 != pl.col("n_unique_iculos")
    ).height
    not_from_1 = patients.filter(pl.col("min_iculos") != 1).height
    non_monotonic = patients.filter(pl.col("label_decreases") > 0).height
    short = patients.filter(pl.col("rows") < 8).height
    long_ = patients.filter(pl.col("max_iculos") > 336).height

    checks = [
        Check(
            "(patient_id, ICULOS) is unique", dup_rows == 0, f"{dup_rows} duplicate rows", dup_rows
        ),
        Check("ICULOS is contiguous per patient", gaps == 0, f"{gaps} patients with gaps", gaps),
        Check(
            "ICULOS starts at 1",
            not_from_1 == 0,
            f"{not_from_1} patients ({not_from_1 / n:.1%}) start later",
            not_from_1,
        ),
        Check(
            "SepsisLabel is monotonic (never 1 -> 0)",
            non_monotonic == 0,
            f"{non_monotonic} patients decrease",
            non_monotonic,
        ),
        Check("Stays have >= 8 hourly rows", short == 0, f"{short} patients with < 8 rows", short),
        Check(
            "ICULOS <= 336 (2-week truncation)", long_ == 0, f"{long_} patients exceed 336", long_
        ),
    ]
    for c in STATIC_COLUMNS:
        varies = patients.filter(pl.col(f"{c}_distinct") > 1).height
        partial = patients.filter(
            (pl.col(f"{c}_nulls") > 0) & (pl.col(f"{c}_nulls") < pl.col("rows"))
        ).height
        checks.append(
            Check(
                f"{c} is constant within a patient",
                varies == 0 and partial == 0,
                f"{varies} patients with >1 value, {partial} partially NULL",
                varies + partial,
            )
        )
    return checks


def _facts(patients: pl.DataFrame, train_frame: pl.DataFrame) -> dict[str, str]:
    """train_frame only needs the FiO2 and Temp columns."""
    n = patients.height
    septic = patients.filter(pl.col("is_sepsis") == 1)
    first_pos = septic["first_positive_row"]
    onset_after_end = septic.filter(pl.col("first_positive_row") + 6 > pl.col("rows") - 1).height
    units = (
        patients.with_columns(
            unit=pl.when((pl.col("Unit1_first") == 1) & (pl.col("Unit2_first") == 1))
            .then(pl.lit("BOTH"))
            .when(pl.col("Unit1_first") == 1)
            .then(pl.lit("MICU"))
            .when(pl.col("Unit2_first") == 1)
            .then(pl.lit("SICU"))
            .when(pl.col("Unit1_first").is_null() & pl.col("Unit2_first").is_null())
            .then(pl.lit("UNKNOWN"))
            .otherwise(pl.lit("NEITHER (0/0)"))
        )
        .group_by("unit", "source_set")
        .len()
        .sort("unit", "source_set")
    )
    unit_text = "; ".join(
        f"{r['unit']}/{r['source_set']}: {r['len']}" for r in units.iter_rows(named=True)
    )
    age = patients["Age_first"]
    fio2 = train_frame["FiO2"].drop_nulls()
    temp = train_frame["Temp"].drop_nulls()
    by_set = (
        patients.group_by("source_set")
        .agg(n=pl.len(), septic=pl.col("is_sepsis").sum())
        .sort("source_set")
    )
    return {
        "Sepsis prevalence (patients)": f"{septic.height} / {n} ({septic.height / n:.2%}); "
        + "; ".join(
            f"{r['source_set']}: {r['septic'] / r['n']:.2%}" for r in by_set.iter_rows(named=True)
        ),
        "Septic patients positive from row 0": f"{(first_pos == 0).sum()} (onset left-censored)",
        "First positive row (median, min, max)": (
            f"{first_pos.median():.0f}, {first_pos.min()}, {first_pos.max()}"
        ),
        "t_sepsis after the last recorded row": f"{onset_after_end} septic patients",
        "ICU unit (Unit1/Unit2 of first row) by hospital": unit_text,
        "Age (min / max / count == 100)": (
            f"{age.min():.0f} / {age.max():.0f} / {(age == 100).sum()} patients"
        ),
        "Patients younger than 18": f"{(age < 18).sum()}",
        "HospAdmTime negative (patients)": f"{(patients['HospAdmTime_first'] < 0).mean():.1%}",
        "Rows per stay (median / max)": (
            f"{patients['rows'].median():.0f} / {patients['rows'].max()}"
        ),
        "FiO2 values > 1 (train)": f"{(fio2 > 1).sum()} of {fio2.len()} "
        f"({(fio2 > 1).sum() / max(fio2.len(), 1):.3%}); of which <= 100: "
        f"{((fio2 > 1) & (fio2 <= 100)).sum()}",
        "Temp range (train)": f"{temp.min()} .. {temp.max()} (values < 30: {(temp < 30).sum()})",
    }


def _variable_stats(train_lf: pl.LazyFrame) -> pl.DataFrame:
    """Per-variable distribution over train patients, one column scanned at a time."""
    rows = []
    for variable in [*VITALS, *LABS]:
        train_frame = train_lf.select(variable, "source_set").collect()
        col = train_frame[variable]
        values = col.drop_nulls()
        stats = {
            "variable": variable,
            "category": "vital" if variable in VITALS else "lab",
            "n_obs": values.len(),
            "null_pct": col.null_count() / max(col.len(), 1) * 100,
            **{
                f"null_pct_{s}": train_frame.filter(pl.col("source_set") == s)[
                    variable
                ].null_count()
                / max(train_frame.filter(pl.col("source_set") == s).height, 1)
                * 100
                for s in ("A", "B")
            },
            "min": values.min(),
            **{k: values.quantile(q, interpolation="linear") for k, q in QUANTILES.items()},
            "max": values.max(),
        }
        rows.append(stats)
    return pl.DataFrame(rows)


def _iculos_buckets(frame: pl.DataFrame) -> pl.DataFrame:
    """frame only needs the ICULOS column."""
    total = frame.height
    return pl.DataFrame(
        [
            {
                "iculos": f"{lo}-{hi}" if hi < 10_000 else f">{lo - 1}",
                "rows": (n := frame.filter(pl.col("ICULOS").is_between(lo, hi)).height),
                "pct": n / total * 100,
            }
            for lo, hi in ICULOS_BUCKETS
        ]
    )


def build_profile(lf: pl.LazyFrame, split: pl.DataFrame, quarantined: int) -> Profile:
    patients = _patient_summary(lf)
    train_ids = split.filter(pl.col("split_random") == "train")["patient_id"]
    train_lf = lf.filter(pl.col("patient_id").is_in(train_ids.implode()))
    train_fio2_temp = train_lf.select("FiO2", "Temp").collect()
    split_table = (
        split.group_by("split_random", "source_set", "is_sepsis")
        .len()
        .pivot(on="split_random", index=["source_set", "is_sepsis"], values="len")
        .sort("source_set", "is_sepsis")
    )
    profile = Profile(
        generated_at=datetime.now(UTC),
        overview={
            "patients": patients.height,
            "patient_hours": int(patients["rows"].sum()),
            "patients_A": patients.filter(pl.col("source_set") == "A").height,
            "patients_B": patients.filter(pl.col("source_set") == "B").height,
            "train_patients": train_ids.len(),
            "quarantined_files": quarantined,
        },
        split_table=split_table,
        checks=_checks(patients),
        facts=_facts(patients, train_fio2_temp),
        variable_stats=_variable_stats(train_lf),
        iculos_buckets=_iculos_buckets(lf.select("ICULOS").collect()),
    )
    if patients.height < 40_336:
        profile.notes.append(
            f"Bronze holds {patients.height} patients, not the full 40,336: this profile was run on"
            " a sample or on a partial load. Re-run on the full data before finalising decisions."
        )
    return profile


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _markdown_table(frame: pl.DataFrame) -> str:
    header = "| " + " | ".join(frame.columns) + " |"
    sep = "|" + "|".join("---" for _ in frame.columns) + "|"
    body = ["| " + " | ".join(_fmt(v) for v in row) + " |" for row in frame.iter_rows()]
    return "\n".join([header, sep, *body])


def render_markdown(profile: Profile) -> str:
    o = profile.overview
    lines = [
        "# Data profile — PhysioNet/CinC 2019 (bronze layer)",
        "",
        f"_Generated by `sepsis profile` on {profile.generated_at:%Y-%m-%d %H:%M} UTC. "
        "Do not edit by hand; decisions taken from this profile are recorded in "
        "`docs/PLAN.md` §2 and in the seed `variable_catalog.csv`._",
        "",
    ]
    lines += [f"> **Note:** {n}" for n in profile.notes] + ([""] if profile.notes else [])
    lines += [
        "## 1. Overview",
        "",
        f"- Patients: **{o['patients']:,}** (A: {o['patients_A']:,}, B: {o['patients_B']:,})",
        f"- Patient-hours: **{o['patient_hours']:,}**",
        f"- Quarantined files: **{o['quarantined_files']}**",
        f"- Train patients (basis for value distributions): **{o['train_patients']:,}**",
        "",
        "Patient split (`silver.patient_split`, patients per split by hospital and sepsis status):",
        "",
        _markdown_table(profile.split_table),
        "",
        "## 2. Assumption checks (all patients)",
        "",
        "| Check | Holds | Observed |",
        "|---|---|---|",
        *[
            f"| {c.name} | {'yes' if c.holds else '**no**'} | {c.observed} |"
            for c in profile.checks
        ],
        "",
        "## 3. Dataset facts",
        "",
        "| Fact | Value |",
        "|---|---|",
        *[f"| {k} | {v} |" for k, v in profile.facts.items()],
        "",
        "## 4. Variable distributions (train patients only)",
        "",
        "Null percentages are per patient-hour. Quantiles are over observed (non-null) values.",
        "",
        _markdown_table(profile.variable_stats),
        "",
        "## 5. Rows by hour of ICU stay (all patients)",
        "",
        "Descriptive only (used to size TimescaleDB chunks in Stage 8).",
        "",
        _markdown_table(profile.iculos_buckets),
        "",
    ]
    return "\n".join(lines)


def write_dq_results(conn: psycopg.Connection, run_id, profile: Profile) -> None:
    rows = [("bronze", "assumption", c.name, "violations", float(c.metric)) for c in profile.checks]
    rows += [("bronze", "overview", None, k, float(v)) for k, v in profile.overview.items()]
    stats = profile.variable_stats
    for record in stats.iter_rows(named=True):
        for metric in ("null_pct", "min", *QUANTILES, "max"):
            if record[metric] is not None:
                rows.append(
                    (
                        "bronze_train",
                        "distribution",
                        record["variable"],
                        metric,
                        float(record[metric]),
                    )
                )
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO audit.dq_results (run_id, layer, check_name, variable, metric, value)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (run_id, layer, check, var, metric, value)
                for layer, check, var, metric, value in rows
            ],
        )


def load_inputs(
    conn: psycopg.Connection, bronze_dir: Path
) -> tuple[pl.LazyFrame, pl.DataFrame, int]:
    frame = pl.scan_parquet(bronze_dir / "patient_hour", hive_partitioning=True).select(
        [*SOURCE_COLUMNS, "patient_id", "source_set", "source_row_number"]
    )
    split = pl.DataFrame(
        conn.execute(
            "SELECT patient_id, source_set, is_sepsis, split_random FROM silver.patient_split"
        ).fetchall(),
        schema=["patient_id", "source_set", "is_sepsis", "split_random"],
        orient="row",
    )
    quarantined = conn.execute("SELECT count(*) FROM raw.quarantine").fetchone()[0]
    return frame, split, quarantined
