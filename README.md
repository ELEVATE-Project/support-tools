# locationfetchwithudise

A small Python script that reads a spreadsheet of school UDISE codes and school names, queries an external Elevate entity API to find matching schools, and writes the results to an output Excel file.

## What this project contains

- `helper.py` — main script. Reads `test.xlsx`, queries the API, and writes `output.xlsx`.
- `requirement.txt` — required Python packages (install with pip).

## Purpose / behavior

`helper.py` iterates every row in the input Excel file and performs a POST request to an API endpoint to find an entity that matches the UDISE code. For each row the script records:

- UDISE Code
- School Name (from the input file)
- The name found in the API response (if any)
- Status (Existing School, School Not Found, Error <code>, or Exception)

The script sleeps 0.2 seconds between requests to reduce the chance of rate-limiting.

## Required input

Place an input Excel file in the project root named `test.xlsx` (or change `INPUT_FILE` in `helper.py`). The script expects the workbook to have at least these two columns (exact column names):

- `School UDISE code (11 digits)` — unique identifier (string or numeric). The script casts it to string and strips spaces.
- `School Name` — human-readable school name (used for logging and comparison).

Example row (Excel):

| School UDISE code (11 digits) | School Name       |
|------------------------------:|-------------------|
| 12345678901                   | Example Primary   |

## Output

The script writes results to `output.xlsx` in the project root. Columns written:

- `UDISE Code`
- `School Name`
- `api_school_name`
- `Status`

## Configuration

Open `helper.py` and edit the configuration block near the top:

- `API_URL` — endpoint used for lookups (default: `https://elevate-api.sunbirdsaas.com/entity-management/v1/entities/find`).
- `HEADERS` — set the required tokens here:
  - `internal-access-token` (required by the target API)
  - `x-auth-token` (if required)
  - `content-type` is set to `application/json` by default

Make sure to populate those header values before running the script, otherwise requests will fail or return authentication errors.

If you'd rather not hardcode tokens, you can modify the script to read them from environment variables (recommended for production).

## Dependencies

Install dependencies from `requirement.txt` (project file is named `requirement.txt`):

```bash
python3 -m pip install -r requirement.txt
```

Typical packages required are:

- pandas
- requests
- openpyxl (pandas needs this to read/write Excel files)

If `requirement.txt` is missing packages, install the above individually.

## How to run

From the project root:

```bash
python3 helper.py
```

Watch the console for progress logs like:

Checking: 12345678901 - example school...
12345678901 - example school -> Existing School

When finished, you should see:

✅ Done! Output saved to /path/to/project/output.xlsx

## Troubleshooting

- Missing columns / KeyError: ensure the input Excel has the exact column names `School UDISE code (11 digits)` and `School Name`.
- Empty HEADERS/tokens: fill `HEADERS` in the script or switch to environment variables. The script will return HTTP 401/403 if credentials are invalid.
- API response structure differs: `helper.py` currently expects the API to return a JSON with top-level `result` or `data` which is a list; the first item should contain `metaInformation` → `name`. If your API returns a different structure, update the extraction code accordingly.
- Excel engine errors: install `openpyxl` if pandas complains about Excel engine.

## Suggested improvements (next steps)

- Add CLI arguments (input/output file paths, API tokens, API URL).
- Move tokens to environment variables for security.
- Add unit tests and a small integration test that uses a mocked API.
- Add logging (instead of print) with configurable log level.

## Contact / License

This is a small utility script. Adjust and reuse as needed. No license file included.