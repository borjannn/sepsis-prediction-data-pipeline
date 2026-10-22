"""Stage 4 checks: utility labels vs the official scoring logic, LOCF vs a reference
implementation, and the leakage lineage contract on dbt's manifest."""

import json
import math

import pytest

from sepsis_pipeline.config import get_settings
from sepsis_pipeline.leakage import LEAKAGE_TEST_PATIENTS
from sepsis_pipeline.transform.dbt_runner import run_dbt


def reference_utility(labels, predictions, dt_early=-12, dt_optimal=-6, dt_late=3.0,
                      max_u_tp=1, min_u_fn=-2, u_fp=-0.05, u_tn=0):  # fmt: skip
    """Independent re-implementation of compute_prediction_utility from the official
    evaluate_sepsis_score.py (physionetchallenges/evaluation-2019), same logic and constants."""
    if any(labels):
        is_septic, t_sepsis = True, labels.index(1) - dt_optimal
    else:
        is_septic, t_sepsis = False, math.inf
    m_1 = max_u_tp / (dt_optimal - dt_early)
    b_1 = -m_1 * dt_early
    m_2 = -max_u_tp / (dt_late - dt_optimal)
    b_2 = -m_2 * dt_late
    m_3 = min_u_fn / (dt_late - dt_optimal)
    b_3 = -m_3 * dt_optimal
    total = 0.0
    for t, predicted in enumerate(predictions):
        if t > t_sepsis + dt_late:
            continue
        if is_septic and predicted:
            if t <= t_sepsis + dt_optimal:
                total += max(m_1 * (t - t_sepsis) + b_1, u_fp)
            else:
                total += m_2 * (t - t_sepsis) + b_2
        elif not is_septic and predicted:
            total += u_fp
        elif is_septic and not predicted:
            if t > t_sepsis + dt_optimal:
                total += m_3 * (t - t_sepsis) + b_3
        else:
            total += u_tn
    return total


def reference_locf(values, hours, cap):
    """Capped last observation carried forward, the obvious loop."""
    last_value = last_hour = None
    out = []
    for value, hour in zip(values, hours, strict=True):
        if value is not None:
            last_value, last_hour = value, hour
        since = None if last_hour is None else hour - last_hour
        out.append((last_value if since is not None and since <= cap else None, since))
    return out


def test_reference_utility_matches_published_examples():
    # Septic at row 20 (labels from row 14): perfect early alarm earns the maximum utility.
    labels = [0] * 14 + [1] * 10
    best = [1 if 8 <= t <= 23 else 0 for t in range(24)]
    assert reference_utility(labels, best) > reference_utility(labels, [0] * 24)
    assert reference_utility([0] * 10, [1] * 10) == pytest.approx(-0.5)
    assert reference_utility([0] * 10, [0] * 10) == 0


@pytest.mark.db
def test_utility_columns_match_official_logic(db_conn):
    patients = db_conn.execute(
        """
        (SELECT patient_id FROM silver.int_patient_sepsis WHERE onset_left_censored LIMIT 3)
        UNION ALL (SELECT patient_id FROM silver.int_patient_sepsis
                   WHERE onset_after_record_end LIMIT 3)
        UNION ALL (SELECT patient_id FROM silver.int_patient_sepsis
                   WHERE is_sepsis_patient AND first_iculos > 1 LIMIT 3)
        UNION ALL SELECT unnest(%s::text[])
        """,
        (list(LEAKAGE_TEST_PATIENTS),),
    ).fetchall()
    assert len(patients) >= 15
    for (patient_id,) in patients:
        rows = db_conn.execute(
            "SELECT label_sepsis, label_utility_if_positive, label_utility_if_negative"
            " FROM silver.int_patient_hour_labels WHERE patient_id = %s ORDER BY row_idx",
            (patient_id,),
        ).fetchall()
        labels = [r[0] for r in rows]
        n = len(labels)
        assert sum(r[1] for r in rows) == pytest.approx(reference_utility(labels, [1] * n))
        assert sum(r[2] for r in rows) == pytest.approx(reference_utility(labels, [0] * n))
        # Mixed predictions: predicting the label itself.
        mixed = sum(r[1] if r[0] else r[2] for r in rows)
        assert mixed == pytest.approx(reference_utility(labels, labels))


@pytest.mark.db
def test_locf_matches_reference_implementation(db_conn):
    caps = dict(db_conn.execute("SELECT variable, locf_max_hours FROM silver.variable_catalog"))
    for variable in ("hr", "temp", "lactate", "wbc", "fio2", "troponin_i"):
        for patient_id in LEAKAGE_TEST_PATIENTS:
            rows = db_conn.execute(
                f"SELECT iculos, {variable}, {variable}_last, {variable}_hours_since"
                " FROM silver.int_patient_hour_imputed WHERE patient_id = %s ORDER BY iculos",
                (patient_id,),
            ).fetchall()
            expected = reference_locf([r[1] for r in rows], [r[0] for r in rows], caps[variable])
            actual = [(r[2], r[3]) for r in rows]
            assert actual == expected, (variable, patient_id)


def test_feature_models_have_the_contracted_parents():
    """Lineage part of the leakage contract (docs/PROJECT.md §9), checked on dbt's manifest."""
    result = run_dbt(["parse"], capture=True)
    assert result.returncode == 0, result.stdout[-2000:]
    manifest_path = get_settings().dbt_project_dir / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    nodes = manifest["nodes"]

    def node(name):
        return nodes[f"model.sepsis_dw.{name}"]

    def parents(name):
        return {nodes[p]["name"] if p in nodes else p for p in node(name)["depends_on"]["nodes"]}

    seeds = {n["name"] for n in nodes.values() if n["resource_type"] == "seed"}
    assert parents("int_patient_hour_clean") <= {"int_measurement_long", "stg_patient_hour"} | seeds
    assert parents("int_patient_hour_imputed") <= {"int_patient_hour_clean"} | seeds
    assert parents("int_patient_hour_features") <= {"int_patient_hour_imputed"} | seeds

    # No label/outcome-tagged node anywhere upstream of the features.
    def upstream(unique_id, seen=None):
        seen = set() if seen is None else seen
        for parent in nodes.get(unique_id, {}).get("depends_on", {}).get("nodes", []):
            if parent not in seen:
                seen.add(parent)
                upstream(parent, seen)
        return seen

    for name in ("int_patient_hour_clean", "int_patient_hour_imputed", "int_patient_hour_features"):
        closure = upstream(f"model.sepsis_dw.{name}")
        tagged = [
            u for u in closure if {"label", "outcome"} & set(nodes.get(u, {}).get("tags", []))
        ]
        assert not tagged, (name, tagged)

    # The compiled SQL of the feature models never mentions the target.
    result = run_dbt(["compile", "--select", "tag:feature", "int_patient_hour_clean"], capture=True)
    assert result.returncode == 0, result.stdout[-2000:]
    compiled = get_settings().dbt_project_dir / "target" / "compiled" / "sepsis_dw" / "models"
    for name in ("int_patient_hour_clean", "int_patient_hour_imputed", "int_patient_hour_features"):
        sql = (compiled / "silver" / f"{name}.sql").read_text(encoding="utf-8")
        assert "sepsis_label" not in sql.lower(), name
