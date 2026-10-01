"""Data dictionary for the ML export: group, type, description, derivation and shortcut risk
for every column of mart.ml_features_patient_hour."""

import re

import polars as pl
import psycopg

DERIVED = {
    "shock_index": "Heart rate / systolic blood pressure (LOCF values)",
    "pulse_pressure": "Systolic - diastolic blood pressure (LOCF values), mmHg",
    "map_or_derived": "MAP (LOCF), or (SBP + 2*DBP) / 3 when MAP is missing",
    "map_is_derived": "map_or_derived was computed from SBP/DBP",
    "bun_creatinine_ratio": "BUN / creatinine (LOCF values)",
    "spo2_fio2_ratio": "SpO2 / FiO2 (LOCF values); PaO2/FiO2 proxy (Rice et al. 2007)",
    "n_vitals_measured": "Number of vital signs measured this hour",
    "n_labs_measured": "Number of lab values measured this hour",
    "n_abnormal": "Number of variables outside their normal range this hour",
    "sirs_temp": "SIRS criterion: temperature > 38 or < 36 deg C",
    "sirs_hr": "SIRS criterion: heart rate > 90",
    "sirs_resp": "SIRS criterion: respiration > 20 or PaCO2 < 32 mmHg",
    "sirs_wbc": "SIRS criterion: WBC > 12 or < 4 x10^3/uL",
    "qsofa_resp": "qSOFA criterion: respiration >= 22",
    "qsofa_sbp": "qSOFA criterion: systolic BP <= 100",
    "sofa_coagulation": "SOFA coagulation score from platelets (0-4)",
    "sofa_liver": "SOFA liver score from total bilirubin (0-4)",
    "sofa_cardiovascular": "SOFA cardiovascular score, MAP < 70 only (0-1; no vasopressor data)",
    "sofa_renal": "SOFA renal score from creatinine (0-4)",
    "sofa_respiratory": "SOFA respiratory score from SpO2/FiO2 (Pandharipande et al. 2009, 0-4)",
    "sirs_count": "Number of SIRS criteria met (0-4)",
    "sirs_components_available": "Number of SIRS criteria that could be evaluated",
    "qsofa_partial": "Partial qSOFA (0-2; mentation not recorded)",
    "qsofa_components_available": "Number of qSOFA criteria that could be evaluated",
    "sofa_partial": "Sum of available SOFA components",
    "sofa_components_available": "Number of SOFA components that could be evaluated",
    "delta_sofa_partial_24h": "sofa_partial minus its minimum over the previous 24 hours",
    "lab_draw_hours_24h": "Hours in the last 24 with at least one lab measured",
    "hours_since_any_lab": "Hours since any lab value was measured",
    "n_abnormal_cumulative": "Running total of abnormal values since the start of the record",
    "lactate_max_so_far": "Highest lactate observed so far in the record",
}
META = {
    "meta_hospital": "Hospital system (A or B)",
    "meta_icu_unit": "ICU unit (MICU, SICU, UNKNOWN)",
    "meta_split_random": "Patient-level split: train / validation / test (70/15/15, stratified)",
    "meta_cv_fold": "Patient-level CV fold 1-5 for train patients (NULL otherwise)",
    "meta_split_hospital_holdout": "Hospital A train/validation (70/30), hospital B = test",
}
LABELS = {
    "label_sepsis": "TARGET. Official SepsisLabel: 1 from 6 h before t_sepsis onwards",
    "label_is_sepsis_patient": "Whole-stay outcome: the patient develops sepsis",
    "label_hours_to_onset": "t_sepsis - row index, in hours (NULL for non-septic patients)",
    "label_utility_if_positive": "Official challenge utility of predicting 1 at this hour",
    "label_utility_if_negative": "Official challenge utility of predicting 0 at this hour",
}
STATS = {"mean": "mean", "min": "minimum", "max": "maximum", "std": "standard deviation",
         "count": "number of observations"}  # fmt: skip
INTENSITY = {"n_vitals_measured", "n_labs_measured", "lab_draw_hours_24h", "hours_since_any_lab"}
ROLLING = re.compile(r"^(?P<v>.+)_(?P<stat>mean|min|max|std|count)_(?P<w>6h|24h)$")
SLOPE = re.compile(r"^(?P<v>.+)_slope_24h$")
DELTA = re.compile(r"^(?P<v>.+)_delta_(?P<w>1h|6h)$")


def hospital_dependent_variables(conn: psycopg.Connection, variables: list[str]) -> dict[str, str]:
    """Variables whose missingness differs strongly between hospitals (train patients)."""
    exprs = ", ".join(f"avg(({v} is null)::int)::float8 AS {v}" for v in variables)
    rows = conn.execute(
        f"SELECT clean.source_set, {exprs} FROM silver.int_patient_hour_clean clean"
        " JOIN silver.patient_split split USING (patient_id)"
        " WHERE split.split_random = 'train' GROUP BY clean.source_set ORDER BY 1"
    ).fetchall()
    missing = {row[0]: dict(zip(variables, row[1:], strict=True)) for row in rows}
    risky = {}
    for v in variables:
        a, b = missing.get("A", {}).get(v), missing.get("B", {}).get(v)
        if a is None or b is None:
            continue
        if abs(a - b) > 0.30 or max(a, b) > 0.995 > min(a, b) + 0.05:
            risky[v] = f"missing {a:.0%} at hospital A vs {b:.0%} at B"
    return risky


def build_dictionary(
    columns: list[tuple[str, str]], catalog: pl.DataFrame, hospital_risk: dict[str, str]
) -> pl.DataFrame:
    """catalog: variable, display_name, unit, locf_max_hours (from the seed)."""
    info = {r["variable"]: r for r in catalog.iter_rows(named=True)}
    rows = []
    for name, pg_type in columns:
        group, description, source, derivation, cap, risk = "", "", None, "", None, ""
        if name in ("patient_id", "iculos"):
            group = "key"
            description = (
                "Patient identifier" if name == "patient_id" else "Hours since ICU admission"
            )
        elif name.startswith("meta_"):
            group, description = "meta", META.get(name, "")
            if name == "meta_hospital":
                risk = "hospital identity; generalisation to new hospitals needs the holdout split"
        elif name.startswith("label_"):
            group, description = "label", LABELS.get(name, "")
        elif name.startswith("feat_"):
            group = "feat"
            base = name.removeprefix("feat_")
            static = {"iculos": "Hours since ICU admission", "age": "Age in years (100 = 90+)",
                      "gender": "0 = female, 1 = male",
                      "hosp_adm_time": "Hours between hospital and ICU admission"}  # fmt: skip
            suffix_match = re.match(r"^(?P<v>.+?)_(?P<kind>measured|hours_since|last)$", base)
            if base in static:
                description, derivation = static[base], "static / time column"
                if base == "iculos":
                    risk = "time-in-stay; 21.8% of records start after ICU hour 1"
            elif base in info:
                v = info[base]
                description = f"{v['display_name']} ({v['unit']}) observed this hour"
                source, derivation = base, "cleaned (out-of-range -> NULL)"
            elif suffix_match and suffix_match["v"] in info:
                v, kind = info[suffix_match["v"]], suffix_match["kind"]
                source = suffix_match["v"]
                if kind == "measured":
                    description = f"{v['display_name']} was measured this hour"
                    risk = "measurement intensity (clinician behaviour, informative missingness)"
                elif kind == "hours_since":
                    description = f"Hours since {v['display_name']} was last measured (uncapped)"
                    risk = "measurement intensity (clinician behaviour, informative missingness)"
                else:
                    cap = v["locf_max_hours"]
                    description = f"Last observed {v['display_name']} ({v['unit']})"
                    derivation = f"LOCF, capped at {cap} h"
            elif base in DERIVED:
                description, derivation = DERIVED[base], "derived (current + past hours)"
                if base in INTENSITY:
                    risk = "measurement intensity (clinician behaviour, informative missingness)"
            elif m := ROLLING.match(base):
                description = (
                    f"{STATS[m['stat']].capitalize()} of observed {m['v']} over the last {m['w']}"
                )
                source, derivation = m["v"], f"rolling window {m['w']} (RANGE on iculos)"
            elif m := SLOPE.match(base):
                description = f"Linear trend (per hour) of observed {m['v']} over the last 24h"
                source, derivation = m["v"], "regr_slope over 24h"
            elif m := DELTA.match(base):
                description = f"Change of LOCF {m['v']} versus {m['w']} earlier"
                source, derivation = m["v"], f"lag {m['w']}"
            if source in hospital_risk:
                risk = "; ".join(filter(None, [risk, hospital_risk[source]]))
        rows.append({
            "column": name, "dtype": pg_type, "group": group, "description": description,
            "source_variable": source, "derivation": derivation, "locf_cap_hours": cap,
            "shortcut_risk": risk,
        })  # fmt: skip
    return pl.DataFrame(rows, schema_overrides={"locf_cap_hours": pl.Int32})


def dictionary_markdown(dictionary: pl.DataFrame) -> str:
    lines = [
        "# Data dictionary — ml_features_patient_hour",
        "",
        "Column groups: **key**, **meta** (not features by default), **feat** (model inputs), "
        "**label** (targets; never inputs). `shortcut_risk` marks features that may encode "
        "hospital or clinician behaviour rather than physiology.",
        "",
        "| column | group | type | description | derivation | shortcut risk |",
        "|---|---|---|---|---|---|",
    ]
    for r in dictionary.iter_rows(named=True):
        lines.append(
            f"| `{r['column']}` | {r['group']} | {r['dtype']} | {r['description']} | "
            f"{r['derivation'] or ''} | {r['shortcut_risk'] or ''} |"
        )
    return "\n".join(lines) + "\n"
