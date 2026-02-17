Ursa Major Choir ETL — minimal prototype

## What this project does now

- Loads configuration from a local `.env` file.
- Connects to two Google Sheets:
  - **RAW** — source spreadsheet (`RAW_SPREADSHEET_ID`).
  - **DB** — target spreadsheet with curated tabs (`TARGET_SPREADSHEET_ID`).
- Ensures a `members` tab exists in the DB spreadsheet and writes a small test table there (no real personal data).

## Setup

1. **Create and activate a virtual environment (optional but recommended)**  
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies**  
   ```bash
   pip install -r requirements.txt
   ```

3. **Create `.env` from the template**  
   ```bash
   cp .env.example .env
   ```

4. **Configure environment variables in `.env`**
   - `RAW_SPREADSHEET_ID` — ID of the source (RAW) Google Sheet.
   - `TARGET_SPREADSHEET_ID` — ID of the target (DB) Google Sheet.
   - `GOOGLE_SERVICE_ACCOUNT_FILE` — path to the local JSON key file for the service account  
     (for example: `./ursa-major-choir-etl-xxxx.json`).  
   - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — reserved for future Telegram alerts.

   Secrets (`.env` and the JSON key file) are already ignored by git and must not be committed.

## How to run the ETL prototype

From the project root:

```bash
python -m etl.main
```

What happens on run:
- The script connects to RAW and DB spreadsheets.
- In the DB spreadsheet it ensures a `members` tab exists.
- It overwrites `members!A1:C2` with a small test table:
  - header row: `member_id`, `full_name`, `is_active`
  - one dummy row with test values.
- It builds dim/fact tables from RAW and **marts** from dim/fact. `fact_attendance` is **full**: one row per (chorister, rehearsal_date), including empty cells (recorded as missed).

The operation is idempotent: repeated runs do not create duplicate rows, they simply overwrite the same ranges.

