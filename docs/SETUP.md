# Setup Guide

This guide sets up a development environment for the sepsis data warehouse pipeline. It is written for **Windows 11 + PowerShell** (the primary dev machine). The macOS/Linux equivalents are given where they differ.

> **Note:** the project is in the planning phase. This guide describes the target setup. Commands marked *(Stage N)* become available once that stage of [`PLAN.md`](PLAN.md) is implemented.

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| **Docker Desktop** | 4.x (Engine 29+) | WSL 2 backend, Linux containers. WSL 2 gets only 50% of host RAM by default, so size it in `.wslconfig` (see §5). |
| **Python** | 3.14 (3.13 fallback) | The project's `.venv` already uses 3.14. |
| **uv** | latest | Dependency locking and installs: `pip install uv` or `winget install astral-sh.uv` |
| **Git** | 2.40+ | |
| **Disk space** | ~10 GB free | Raw files ~0.3 GB; Postgres with all layers plus the Timescale copy: a few GB; Parquet exports < 1 GB. |
| *Optional:* a DB client | — | PyCharm Professional's Database tool or DBeaver |

No local PostgreSQL install is needed. The database runs in Docker, and `psql` is available inside the container.

---

## 2. Get the code

```powershell
git clone <repo-url> sepsis-dw
cd sepsis-dw
```

(If you're starting from this folder before the repo exists: `git init` first.)

---

## 3. Python environment

```powershell
uv sync --extra dev               # creates/updates .venv from uv.lock: package + dbt + dev tools  (Stage 0)
.\.venv\Scripts\Activate.ps1
```

macOS/Linux: `uv sync --extra dev && source .venv/bin/activate`.

`uv.lock` pins every dependency, so all teammates get identical versions. If a package has no Python 3.14 wheel for Windows, run `uv python pin 3.13` and then `uv sync` again.

If PowerShell blocks `Activate.ps1`, run this once: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

**PyCharm:** Settings → Project → Python Interpreter → select `.venv\Scripts\python.exe`. Mark `src/` as *Sources Root*.

---

## 4. Configuration (`.env`)

Copy the template and adjust it if needed:

```powershell
Copy-Item .env.example .env
```

Expected variables:

```dotenv
# --- database (docker-compose reads these too) ---
POSTGRES_HOST=localhost
POSTGRES_PORT=5432            # change to 5433 if a local Postgres already uses 5432
POSTGRES_DB=sepsis_dw
POSTGRES_USER=etl_writer
POSTGRES_PASSWORD=change-me
DM_READER_PASSWORD=change-me-too

# --- pipeline ---
DATA_DIR=./data
EXPORT_DIR=./exports
EXTRACT_WORKERS=16
SPLIT_SEED=42
SPLIT_VERSION=1
SAMPLE_SIZE=400               # default for --sample

# --- dbt ---
DBT_PROFILES_DIR=./dbt/sepsis_dw
```

`.env` is gitignored. Never commit real passwords.

---

## 5. Start the warehouse database

**WSL 2 memory (Windows, one-time):** create `%UserProfile%\.wslconfig` containing:

```ini
[wsl2]
memory=8GB        # adjust to your machine; ~half to two-thirds of physical RAM
processors=4
```

Then run `wsl --shutdown` and restart Docker Desktop. The compose file sets `shm_size: 1g` for the container, because Postgres parallel queries fail with the default 64 MB `/dev/shm`.

```powershell
docker compose up -d                       # starts timescale/timescaledb:2.30.2-pg18   (Stage 0)
docker compose ps                          # STATUS should be "healthy"
```

Verify the schemas and the TimescaleDB extension:

```powershell
docker compose exec warehouse psql -U etl_writer -d sepsis_dw -c "\dn" -c "\dx"
```

You should see the schemas `raw, staging, silver, gold, mart, audit, ts` and the extension `timescaledb`.

**Reset the database** (⚠ deletes all warehouse data; the init scripts re-run on the next start):

```powershell
docker compose down -v
docker compose up -d
```

---

## 6. dbt setup

```powershell
$env:DBT_PROFILES_DIR = "$PWD\dbt\sepsis_dw"   # or rely on .env via the sepsis CLI
cd dbt\sepsis_dw
dbt deps          # installs dbt_utils                 (Stage 0)
dbt debug         # "All checks passed!"               (Stage 0)
cd ..\..
```

macOS/Linux: `export DBT_PROFILES_DIR="$PWD/dbt/sepsis_dw"`.

---

## 7. Health check

```powershell
sepsis doctor     # DB reachable, timescaledb present, dbt debug OK, data dirs writable   (Stage 0)
```

---

## 8. Running the pipeline

Run the stages one at a time:

```powershell
sepsis extract                 # download + verify ~40k .psv files          (Stage 1)
sepsis load-bronze             # parse, validate, load raw.*                (Stage 2)
sepsis profile                 # writes docs/data_profile.md                (Stage 2)
sepsis transform               # dbt build: staging → silver → gold → marts (Stages 3–6)
sepsis export                  # Parquet + data dictionary + manifest       (Stage 7)
sepsis timescale               # TimescaleDB add-on + benchmark             (Stage 8)
```

Or run everything:

```powershell
sepsis run-all                 # stages 1–7                                 (Stage 9)
sepsis run-all --with-timescale
```

**Fast dev loop:** add `--sample 400` to work on a 400-patient subset (≈ 28 septic patients), stratified by hospital and sepsis status.

**dbt directly** (for model development):

```powershell
cd dbt\sepsis_dw
dbt build --select staging+            # build + test everything downstream of staging
dbt build --select int_patient_hour_imputed
dbt docs generate; dbt docs serve      # lineage graph at http://localhost:8080
```

---

## 9. Running tests

```powershell
pytest                         # unit + leakage/lineage/LOCF/utility tests + e2e on a sample (creates/drops a sepsis_test DB)
cd dbt\sepsis_dw; dbt test     # data tests against the warehouse
```

---

## 10. Connecting from a SQL client

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` (or your `POSTGRES_PORT`) |
| Database | `sepsis_dw` |
| User / password | `etl_writer` / from `.env` (full access) or `dm_reader` (read-only on `gold`, `mart`) |

---

## 11. For the Data Mining team

You do **not** need to run the warehouse to start working:

1. **Parquet exports (recommended):** get the latest `exports/v*/` folder from the shared drive. Read `manifest.json` and `data_dictionary.md` first.
   ```python
   import polars as pl
   train = pl.read_parquet("exports/v1_YYYYMMDD/ml_features_patient_hour/meta_split_random=train/")
   ```
   **Rules:**
   - Only `feat_*` columns are model inputs.
   - `label_*` (targets) and `outcome_*` (whole-stay facts) must never be used as inputs.
   - `meta_*` columns are not features by default.
   - Split and cross-validate **by patient**: use `meta_cv_fold`, never row-level K-fold.
   - For scaling or imputation, use `train_reference_stats` for your split scheme.
2. **Rebuild locally:** follow sections 1–8. You'll get the identical warehouse, since the split is deterministic for a given `SPLIT_SEED`.
3. **Read-only DB access** on the same network: connect as `dm_reader` to the host machine's IP. The host must allow it through Windows Firewall on the DB port.

---

## 12. Troubleshooting

| Symptom | Fix |
|---|---|
| `port is already allocated` / connection refused on 5432 | A local PostgreSQL is running. Set `POSTGRES_PORT=5433` in `.env` and restart compose. |
| Init SQL didn't run (schemas missing) | Init scripts only run on an **empty** volume. Run `docker compose down -v && docker compose up -d` (deletes data). |
| `dbt: command not found` | Activate the venv. Check `pip show dbt-core`. |
| `Could not find profile named 'sepsis_dw'` | `DBT_PROFILES_DIR` is not set or points to the wrong folder. |
| A dependency fails to install on Python 3.14 | Create a 3.13 venv (`py -3.13 -m venv .venv`). Report which package failed, so the plan's risk table can be updated. |
| Slow dbt models / Postgres OOM-killed | Raise the WSL 2 memory in `.wslconfig`. Lower `work_mem` in `docker-compose.yml`. Develop with `--sample`. |
| `could not resize shared memory segment` | `shm_size` is missing or too small in `docker-compose.yml`. Should be `1g`. |
| Extract stalls / HTTP 503 | The download retries automatically and is resumable, so just re-run `sepsis extract`. Lower `EXTRACT_WORKERS` if it persists. |
| Shell scripts in `sql/init` fail with `\r` errors | CRLF line endings. `.gitattributes` enforces `eol=lf` for `*.sh`/`*.sql`; re-checkout or convert the file. |
