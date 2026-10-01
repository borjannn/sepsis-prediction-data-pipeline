"""Stage 7 — Export a versioned, self-describing dataset for the Data Mining team."""

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from psycopg import sql

from sepsis_pipeline import __version__
from sepsis_pipeline.audit import current_git_sha
from sepsis_pipeline.config import Settings
from sepsis_pipeline.db import connect
from sepsis_pipeline.export.dictionary import (
    build_dictionary,
    dictionary_markdown,
    hospital_dependent_variables,
)
from sepsis_pipeline.export.parquet import export_query, relation_columns, sha256
from sepsis_pipeline.export.reference_stats import build_reference_stats

SPLITS = ("train", "validation", "test")
EXPORT_BATCH_PATIENTS = 2_500

OUTCOME_QUERY = """
SELECT
    patient.patient_id,
    hospital.hospital_code AS meta_hospital,
    unit.unit_code AS meta_icu_unit,
    stay.admit_stay_hour_key AS outcome_first_iculos,
    stay.last_stay_hour_key AS outcome_last_iculos,
    stay.icu_los_hours AS outcome_icu_los_hours,
    stay.n_hours_recorded AS outcome_n_hours_recorded,
    stay.is_sepsis_patient AS outcome_is_sepsis_patient,
    stay.first_positive_stay_hour_key AS outcome_first_positive_iculos,
    stay.onset_stay_hour_key AS outcome_sepsis_onset_iculos,
    stay.onset_left_censored AS outcome_onset_left_censored,
    stay.onset_after_record_end AS outcome_onset_after_record_end,
    stay.n_vital_observations AS outcome_n_vital_observations,
    stay.n_lab_observations AS outcome_n_lab_observations,
    stay.max_lactate AS outcome_max_lactate,
    stay.min_map AS outcome_min_map,
    stay.max_hr AS outcome_max_hr,
    stay.max_temp AS outcome_max_temp,
    stay.min_platelets AS outcome_min_platelets,
    stay.max_sofa_partial AS outcome_max_sofa_partial,
    stay.max_sirs_count AS outcome_max_sirs_count,
    stay.pct_hours_with_full_vitals AS outcome_pct_hours_with_full_vitals
FROM gold.fact_icu_stay stay
JOIN gold.dim_patient patient USING (patient_key)
JOIN gold.dim_hospital hospital USING (hospital_key)
JOIN gold.dim_icu_unit unit USING (unit_key)
ORDER BY patient.patient_id
"""

LICENSE_TEXT = """# Data license and attribution

This export is derived from the **PhysioNet/Computing in Cardiology Challenge 2019** dataset,
version 1.0.0, DOI [10.13026/v64v-d857](https://doi.org/10.13026/v64v-d857).

- The PhysioNet project page lists the **Creative Commons Attribution 4.0 International**
  license; the bundled `LICENSE.txt` is the **Open Database License (ODbL) v1.0**. This derived
  database is shared under the same terms: keep this attribution, and share derived databases
  under ODbL.

Please cite:

- Reyna, M. A., Josef, C. S., Jeter, R., Shashikumar, S. P., Westover, M. B., Nemati, S.,
  Clifford, G. D., & Sharma, A. (2020). Early Prediction of Sepsis From Clinical Data: The
  PhysioNet/Computing in Cardiology Challenge 2019. *Critical Care Medicine*, 48(2), 210-217.
- Goldberger, A., et al. (2000). PhysioBank, PhysioToolkit, and PhysioNet. *Circulation*,
  101(23), e215-e220.
"""


@dataclass
class ExportSummary:
    out_dir: Path
    rows_per_split: dict[str, int]
    files: int


def _export_relation(conn, schema: str, relation: str, out_path: Path, tmp_dir: Path) -> int:
    columns = relation_columns(conn, schema, relation)
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(schema, relation))
    return export_query(conn, query, columns, out_path, tmp_dir)


def run_export(settings: Settings) -> ExportSummary:
    stamp = datetime.now(UTC)
    out_dir = settings.export_dir / f"v{settings.split_version}_{stamp:%Y%m%d}"
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    tmp_dir = settings.data_dir / "tmp"
    rows_per_split: dict[str, int] = {}

    with connect(settings) as conn:
        mart_columns = relation_columns(conn, "mart", "ml_features_patient_hour")
        # Hive-style folders (meta_split_random=<split>), but the column is also kept inside the
        # files, so reading one split folder on its own still has it (Polars accepts both).
        file_columns = mart_columns
        select_list = sql.SQL(", ").join(sql.Identifier(c[0]) for c in file_columns)
        for split in SPLITS:
            # Batches of patients bound memory: converting the whole ~280-column split at once
            # peaked at several GB. One Parquet part per batch; readers use the directory.
            patients = [
                r[0]
                for r in conn.execute(
                    "SELECT patient_id FROM silver.patient_split WHERE split_random = %s"
                    " ORDER BY patient_id",
                    (split,),
                )
            ]
            written = 0
            for part, start in enumerate(range(0, len(patients), EXPORT_BATCH_PATIENTS)):
                batch = patients[start : start + EXPORT_BATCH_PATIENTS]
                query = sql.SQL(
                    "SELECT {} FROM mart.ml_features_patient_hour"
                    " WHERE meta_split_random = {} AND patient_id = ANY({})"
                    " ORDER BY patient_id, iculos"
                ).format(select_list, sql.Literal(split), sql.Literal(batch))
                path = (
                    out_dir
                    / "ml_features_patient_hour"
                    / f"meta_split_random={split}"
                    / f"part-{part:04d}.parquet"
                )
                written += export_query(conn, query, file_columns, path, tmp_dir)
            expected = conn.execute(
                "SELECT count(*) FROM mart.ml_features_patient_hour WHERE meta_split_random = %s",
                (split,),
            ).fetchone()[0]
            if written != expected:
                raise RuntimeError(f"{split}: wrote {written} rows, database has {expected}")
            rows_per_split[split] = written

        # Train-only reference statistics, from the train export (also loaded into the DB).
        stats = build_reference_stats(
            conn, out_dir / "ml_features_patient_hour" / "meta_split_random=train"
        )
        stats.write_parquet(out_dir / "train_reference_stats.parquet", compression="zstd")
        _export_relation(
            conn, "mart", "utility_normalisers", out_dir / "utility_normalisers.parquet", tmp_dir
        )
        _export_relation(
            conn, "silver", "patient_split", out_dir / "patient_split.parquet", tmp_dir
        )
        outcome_columns = [
            (d.name, d.type_display or "text")
            for d in conn.execute(OUTCOME_QUERY + " LIMIT 0").description
        ]
        export_query(
            conn, sql.SQL(OUTCOME_QUERY), _pg_types(conn, outcome_columns),
            out_dir / "icu_stay_outcomes.parquet", tmp_dir,
        )  # fmt: skip

        catalog = pl.DataFrame(
            conn.execute(
                "SELECT variable, display_name, unit, locf_max_hours FROM silver.variable_catalog"
            ).fetchall(),
            schema=["variable", "display_name", "unit", "locf_max_hours"],
            orient="row",
        )
        risk = hospital_dependent_variables(conn, catalog["variable"].to_list())
        dictionary = build_dictionary(mart_columns, catalog, risk)
        strata = conn.execute(
            "SELECT split_random, source_set, is_sepsis, count(*) FROM silver.patient_split"
            " GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"
        ).fetchall()

    dictionary.write_csv(out_dir / "data_dictionary.csv")
    (out_dir / "data_dictionary.md").write_text(dictionary_markdown(dictionary), encoding="utf-8")
    (out_dir / "LICENSE-DATA.md").write_text(LICENSE_TEXT, encoding="utf-8")

    files = sorted(p for p in out_dir.rglob("*") if p.is_file())
    manifest = {
        "dataset": "PhysioNet/CinC Challenge 2019 v1.0.0 - sepsis data warehouse export",
        "exported_at": stamp.isoformat(),
        "pipeline_version": __version__,
        "git_sha": current_git_sha(),
        "split_version": str(settings.split_version),
        "split_seed": str(settings.split_seed),
        "rows_per_split": rows_per_split,
        "patients_per_split_stratum": [
            {"split": s, "hospital": h, "is_sepsis": int(e), "patients": n} for s, h, e, n in strata
        ],
        "column_groups": dictionary.group_by("group").len().sort("group").rows(),
        "columns": [{"name": n, "dtype": t} for n, t in mart_columns],
        "files": {p.relative_to(out_dir).as_posix(): sha256(p) for p in files},
        "rules": [
            "Only feat_* columns are model inputs; label_* are targets; meta_* are not features"
            " by default; icu_stay_outcomes.parquet is analysis-only (outcome_* never features).",
            "Split and cross-validate by patient (meta_cv_fold), never row-level K-fold.",
            "Scale/impute with train_reference_stats rows of the matching split_scheme.",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return ExportSummary(out_dir=out_dir, rows_per_split=rows_per_split, files=len(files) + 1)


def _pg_types(conn, columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Map psycopg type display names (e.g. 'int4', 'float8') to information_schema names."""
    aliases = {
        "int2": "smallint",
        "int4": "integer",
        "int8": "bigint",
        "float8": "double precision",
        "float4": "real",
        "bool": "boolean",
        "varchar": "character varying",
        "bpchar": "character",
    }
    return [(name, aliases.get(t.split("(")[0], t)) for name, t in columns]
