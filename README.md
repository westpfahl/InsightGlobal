# CMS Hospital Dataset Downloader

Downloads all datasets tagged under the **"Hospitals"** theme from the
[CMS Provider Data metastore](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items),
normalizes column names to `snake_case`, and tracks run state so only
modified files are re-downloaded on subsequent runs.

---

## Requirements

- Python 3.9+
- Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
# Standard daily run
python cms_downloader.py

# Custom output directory and thread count
python cms_downloader.py --output-dir ./hospital_data --workers 8

# Force re-download of all files (ignores state)
python cms_downloader.py --force
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `--output-dir` | `./data` | Directory for CSV output, logs, and state |
| `--workers` | `4` | Number of parallel download threads |
| `--force` | `False` | Re-download all files regardless of state |

---

## Output Structure

```
./data/
├── hospital_general_information.csv       ← processed CSV files
├── complications_and_deaths_hospital.csv
├── ...
├── run_state.json                         ← tracks modified timestamps + MD5s
├── run_report_20240315_083012.json        ← per-run audit report
└── cms_downloader.log                     ← rotating log file
```

---

## Column Renaming (snake_case)

All CSV column headers are normalized on download:

| Original | Renamed |
|---|---|
| `Patients' rating of the facility linear mean score` | `patients_rating_of_the_facility_linear_mean_score` |
| `Hospital Name` | `hospital_name` |
| `ZIP Code` | `zip_code` |
| `% of Patients` | `percent_of_patients` |
| `Provider ID` | `provider_id` |

Rules applied in order:
1. Lowercase the entire string
2. Remove apostrophes and special punctuation
3. Replace runs of non-alphanumeric characters with `_`
4. Strip leading/trailing underscores
5. Deduplicate any resulting collisions by appending `_2`, `_3`, etc.

---

## Change Detection Logic

On each run the script checks two things before downloading:

1. **Modified timestamp** from the CMS API metadata
2. **MD5 hash** of the raw file bytes

A file is skipped if *both* the timestamp and MD5 match the previous run.
The `--force` flag bypasses both checks.

---

## Scheduling

### Linux / macOS — cron

Run daily at 6:00 AM:

```cron
0 6 * * * /usr/bin/python3 /path/to/cms_downloader.py --output-dir /data/cms_hospitals >> /var/log/cms_cron.log 2>&1
```

### Windows — Task Scheduler

```
Action:  Start a program
Program: C:\Python39\python.exe
Arguments: C:\jobs\cms_downloader.py --output-dir C:\data\cms_hospitals
```

Or via PowerShell:

```powershell
$action = New-ScheduledTaskAction -Execute "python.exe" `
    -Argument "C:\jobs\cms_downloader.py --output-dir C:\data\cms_hospitals"
$trigger = New-ScheduledTaskTrigger -Daily -At 6am
Register-ScheduledTask -TaskName "CMS_Hospital_Download" -Action $action -Trigger $trigger
```

---

## Design Decisions & Best Practices

| Decision | Rationale |
|---|---|
| **Rotating log file** | Prevents unbounded disk growth; retains 5 × 5MB = 25MB of history |
| **Atomic state file write** (tmp + rename) | Prevents corrupt state if the process is killed mid-write |
| **MD5 hash check** in addition to modified date | CMS timestamps are not always reliable; hash catches silent content changes |
| **ThreadPoolExecutor** for downloads | I/O-bound work — threads are appropriate; CPU-bound transforms would use ProcessPoolExecutor |
| **Retry with backoff** on HTTP session | CMS API occasionally returns transient 5xx errors |
| **Non-zero exit code on errors** | Enables schedulers and monitoring tools to detect failures automatically |
| **utf-8-sig decode** | Strips BOM characters that CMS CSVs sometimes include, preventing corrupt first column names |
| **Duplicate snake_case detection** | Two columns that normalize to the same name would silently overwrite each other without this guard |

---

## Notes for Healthcare / Production Environments

- If deploying in an environment with PHI, ensure `--output-dir` points to
  an encrypted volume and access is restricted to authorized users.
- The run reports and state files contain dataset titles and URLs but **no
  patient data** — they are safe to store in standard log directories.
- For enterprise deployment, consider replacing the local `run_state.json`
  with a lightweight SQLite database or Azure Table Storage for concurrent
  multi-node safety.
