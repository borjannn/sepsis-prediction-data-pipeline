# Setup Guide

This guide sets up a development environment for the sepsis data preparation pipeline. It is written for **Windows 11 + PowerShell** (the primary dev machine). The macOS/Linux equivalents are given where they differ.

> All stages are implemented. A full run from an empty database takes roughly 25 minutes on a laptop (most of it the first download).
>
> **Only want the dataset for data mining?** You don't need any of this. See [§11](#11-for-the-data-mining-team).

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
uv sync --extra dev               # creates/updates .venv from uv.lock: package + dbt + dev tools
.\.venv\Scripts\Activate.ps1
```

macOS/Linux: `uv sync --extra dev && source .venv/bin/activate`.

`uv.lock` pins every dependency, so all teammates get identical versions. If a package has no Python 3.14 wheel for Windows, run `uv python pin 3.13` and then `uv sync` again.

If PowerShell blocks `Activate.ps1`, run this once: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

**"An Application Control policy has blocked this file" (`sepsis.exe`):** Windows Smart App Control can block the freshly generated, unsigned venv launcher. Use the equivalent module form, which runs through the already-trusted `python.exe`:

```powershell
python -m sepsis_pipeline doctor      # same as: sepsis doctor
```

Every `sepsis …` command in this guide works the same way as `python -m sepsis_pipeline …`. The pipeline runs dbt through `python.exe` too, so it doesn't depend on `dbt.exe` either.

**PyCharm:** Settings → Project → Python Interpreter → select `.venv\Scripts\python.exe`. Mark `src/` as *Sources Root*.

---

## 4. Configuration (`.env`)

Copy the template and adjust it if needed:

```powershell
Copy-Item .env.example .env
```

Expected variables:

See `.env.example` for all variables: database host/port/name, the `etl_writer` and `dm_reader` passwords, data/export folders, extract workers, split seed/version and sample size. Replace both passwords with random values, for example:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(18))"
```

If a local PostgreSQL already uses port 5432, set `POSTGRES_PORT=5433` (or any free port).

`.env` is gitignored. Never commit real passwords. Put comments on their own lines, not after a value.

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
docker compose up -d                       # starts timescale/timescaledb:2.30.2-pg18
docker compose ps                          # STATUS should be "healthy"
```

Verify the schemas and the TimescaleDB extension:

```powershell
docker compose exec warehouse psql -U etl_writer -d sepsis_dw -c "\dn" -c "\dx"
```

You should see the schemas `raw, staging, silver, gold, mart, audit, ts` and the extension `timescaledb`.

Our init scripts (`sql/init/10_schemas.sql`, `20_roles.sh`) are mounted **file by file** into `/docker-entrypoint-initdb.d/`. The image keeps its own TimescaleDB init scripts in that folder, and mounting a whole directory there would hide them.

**Reset the database** (⚠ deletes all warehouse data; the init scripts re-run on the next start):

```powershell
docker compose down -v
docker compose up -d
```

---

## 6. dbt setup

Use the `sepsis dbt` passthrough. It runs any dbt command with the credentials from `.env` and the right project and profiles directories:

```powershell
sepsis dbt deps          # installs dbt_utils
sepsis dbt debug         # "All checks passed!"
```

Running `dbt` directly also works, but then you must export `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` and `DBT_PROFILES_DIR` yourself, because `profiles.yml` reads them with `env_var()`.

---

## 7. Health check

```powershell
sepsis doctor
```

It checks: `.env` present, DB connection, TimescaleDB extension, layer schemas, audit tables, the `dm_reader` role, `dbt debug`, and writable data/export folders. The command exits with code 1 if any check fails.

---

## 8. Running the pipeline

Run the stages one at a time:

```powershell
sepsis extract                 # download + verify ~40k .psv files
sepsis load-bronze             # parse, validate, load raw.*
sepsis profile                 # writes docs/data_profile.md
sepsis transform               # dbt build: staging → silver → gold → marts
sepsis export                  # Parquet + data dictionary + manifest
sepsis timescale               # TimescaleDB add-on + benchmark (full rebuild, ~4 min)
sepsis timescale --resume      # continue an interrupted run from the current ts.* state
```

Or run everything:

```powershell
sepsis run-all                 # extract → bronze → profile → dbt build → export
sepsis run-all --with-timescale
```

**Fast dev loop:** `sepsis load-bronze --sample 400` (or `sepsis run-all --sample 400`) loads a 400-patient subset (≈ 28 septic patients) stratified by hospital and sepsis status, plus the 9 fixed leakage-test patients. In sample mode `run-all` writes the profile to `data/data_profile_sample.md`, so the full-data `docs/data_profile.md` is never overwritten. Run `sepsis load-bronze` (no `--sample`) to go back to the full data.

**Outputs to know:**

| Where | What |
|---|---|
| `docs/data_profile.md` | Profile and assumption checks (from `sepsis profile`) |
| `docs/timescale_benchmark.md` | TimescaleDB benchmark (from `sepsis timescale`) |
| `exports/v<split_version>_<date>/` | The Data Mining package (from `sepsis export`) |
| `audit.etl_run`, `audit.dbt_model_run`, `audit.dq_results` | Run log, per-dbt-node results, data-quality metrics |

**dbt directly** (for model development):

```powershell
sepsis dbt build --select staging+            # build + test everything downstream of staging
sepsis dbt build --select int_patient_hour_imputed
sepsis dbt docs generate
sepsis dbt docs serve                         # lineage graph at http://localhost:8080
```

---

## 9. Running tests

```powershell
python -m pytest               # unit + DB tests (DB tests skip if the warehouse isn't running)
python -m pytest -m "not db"   # unit tests only
python -m pytest -m e2e        # end-to-end on a sample in a throwaway database `sepsis_test`,
                               # including the truncation leakage test (~5 min)
sepsis dbt test                # data tests against the warehouse
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
   everything = pl.read_parquet("exports/v1_YYYYMMDD/ml_features_patient_hour/")  # all splits
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
| `An Application Control policy has blocked this file` | Use `python -m sepsis_pipeline …` instead of `sepsis …` (§3). |
| `Could not find profile named 'sepsis_dw'` / `env var required but not provided` | You ran `dbt` directly. Use `sepsis dbt …`, or export the variables listed in §6. |
| A dependency fails to install on Python 3.14 | Create a 3.13 venv (`py -3.13 -m venv .venv`). Report which package failed. |
| Slow dbt models / Postgres OOM-killed | Raise the WSL 2 memory in `.wslconfig`. Lower `work_mem` in `docker-compose.yml`. Develop with `--sample`. |
| `could not resize shared memory segment` | `shm_size` is missing or too small in `docker-compose.yml`. Should be `1g`. |
| Host runs out of memory during `sepsis timescale` | The WSL VM keeps page cache after large writes. Close other heavy apps or cap WSL memory (§5), then `sepsis timescale --resume`. |
| Extract stalls / HTTP 503 | The download retries automatically and is resumable, so just re-run `sepsis extract`. Lower `EXTRACT_WORKERS` if it persists. |
| Shell scripts in `sql/init` fail with `\r` errors | CRLF line endings. `.gitattributes` enforces `eol=lf` for `*.sh`/`*.sql`; re-checkout or convert the file. |
