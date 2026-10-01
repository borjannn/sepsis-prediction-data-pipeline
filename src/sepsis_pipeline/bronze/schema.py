"""The raw .psv contract: column names/order, per-file validation rules, and the pandera schema."""

import pandera.polars as pa
import polars as pl

VITALS = ["HR", "O2Sat", "Temp", "SBP", "MAP", "DBP", "Resp", "EtCO2"]
LABS = [
    "BaseExcess", "HCO3", "FiO2", "pH", "PaCO2", "SaO2", "AST", "BUN", "Alkalinephos",
    "Calcium", "Chloride", "Creatinine", "Bilirubin_direct", "Glucose", "Lactate", "Magnesium",
    "Phosphate", "Potassium", "Bilirubin_total", "TroponinI", "Hct", "Hgb", "PTT", "WBC",
    "Fibrinogen", "Platelets",
]  # fmt: skip
DEMOGRAPHICS = ["Age", "Gender", "Unit1", "Unit2", "HospAdmTime", "ICULOS"]
LABEL = "SepsisLabel"

SOURCE_COLUMNS = [*VITALS, *LABS, *DEMOGRAPHICS, LABEL]
EXPECTED_HEADER = "|".join(SOURCE_COLUMNS)
SOURCE_SCHEMA = dict.fromkeys(SOURCE_COLUMNS, pl.Float64)

LINEAGE_SCHEMA = {
    "patient_id": pl.String,
    "source_set": pl.String,
    "source_file": pl.String,
    "source_row_number": pl.Int32,
    "file_md5": pl.String,
    "load_run_id": pl.String,
    "loaded_at": pl.Datetime("us", "UTC"),
}

BRONZE_COLUMNS = [*SOURCE_COLUMNS, *LINEAGE_SCHEMA]

# Per-file rules: a file violating any of them is quarantined as a whole (never silently fixed).
# Each expression is evaluated per source_file and must be True for the file to pass.
FILE_RULES: dict[str, pl.Expr] = {
    "has rows": pl.len() > 0,
    # fill_null(False): .all() would otherwise skip NULLs and let a missing label pass.
    "SepsisLabel not null and in {0,1}": pl.col(LABEL).is_in([0.0, 1.0]).fill_null(False).all(),
    "ICULOS not null and >= 1": (pl.col("ICULOS") >= 1).fill_null(False).all(),
    "Gender in {0,1} where present": pl.col("Gender")
    .is_in([0.0, 1.0])
    .or_(pl.col("Gender").is_null())
    .all(),
    "no float NaN (only NULL)": ~pl.any_horizontal(pl.col(SOURCE_COLUMNS).is_nan()).any(),
}

# Final contract on the loaded frame (after quarantine): types, nullability, value domains.
BRONZE_SCHEMA = pa.DataFrameSchema(
    {
        **{c: pa.Column(pl.Float64, nullable=True) for c in SOURCE_COLUMNS},
        LABEL: pa.Column(pl.Float64, pa.Check.isin([0.0, 1.0]), nullable=False),
        "ICULOS": pa.Column(pl.Float64, pa.Check.ge(1), nullable=False),
        "Gender": pa.Column(pl.Float64, pa.Check.isin([0.0, 1.0]), nullable=True),
        "patient_id": pa.Column(pl.String, pa.Check.str_matches(r"^p\d{6}$"), nullable=False),
        "source_set": pa.Column(pl.String, pa.Check.isin(["A", "B"]), nullable=False),
        "source_file": pa.Column(pl.String, nullable=False),
        "source_row_number": pa.Column(pl.Int32, pa.Check.ge(1), nullable=False),
        "file_md5": pa.Column(pl.String, nullable=False),
        "load_run_id": pa.Column(pl.String, nullable=False),
        "loaded_at": pa.Column(pl.Datetime("us", "UTC"), nullable=False),
    },
    strict=True,
    ordered=True,
)
