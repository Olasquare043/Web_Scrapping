# Web_Scrapping

This repository has two workflows:

- `nigeria_profs/`: the original Nigeria workflow, preserved separately
- `run_country_professor_extraction.py`: the generic country pipeline for public lecturer/professor email mining from official higher-institution sources

The generic pipeline is designed so you pass only a country name, and the script creates that country's output folder automatically. Kenya is the first real test case, but the runner is not hard-wired to a Kenya module.

## What The Generic Script Does

For a country such as `Kenya`, `Uganda`, or `Ghana`, the generic pipeline will:

1. discover official higher-education source pages for that country
2. build a queue of higher institutions from those official sources
3. resolve likely official institution domains
4. crawl public official staff, faculty, department, profile, and PDF pages
5. extract publicly posted lecturer or professor names, ranks, and emails
6. export editable CSV and Excel outputs into `<country>_profs/`

The pipeline only uses public data from official sources. It does not use logins, CAPTCHA bypass, or fabricated emails.

## Repository Layout

- `nigeria_profs/`: legacy Nigeria scripts and outputs
- `country_pipelines/`: generic country discovery and crawling engine
- `office_dashboard/`: Dialogic Solution local office interface
- `shared/`: reusable extraction and export helpers
- `run_country_professor_extraction.py`: generic CLI entrypoint
- `run_dialogic_dashboard.py`: dashboard launcher
- `Test/`: temporary smoke runs and debug outputs only

## Install

Use Python 3.11+ if possible.

```bash
python -m pip install -r requirements.txt
```

## Dialogic Solution Office Dashboard

For staff who should run jobs without working in the terminal, launch the branded local dashboard:

```bash
python run_dialogic_dashboard.py
```

or on Windows:

```bash
Launch_Dialogic_Dashboard.bat
```

Default dashboard URL:

```text
http://127.0.0.1:5080/
```

The dashboard lets employees:

- enter a country name
- optionally target specific institutions
- control crawl depth and worker count
- watch live run events in the operator console
- open the output folder
- download CSV and workbook artifacts

Use the dashboard for office operations.
Use the CLI when you want more direct scripting or automation.

## Deploy On Render

This repository now includes both a root `Dockerfile` and a root `render.yaml`.

You can deploy in either of these ways:

- create a Render Web Service and let Render detect the root `Dockerfile`
- or sync the included `render.yaml` Blueprint so Render creates the service with the health check already configured

Basic Render setup:

1. Create a new Web Service from this GitHub repository.
2. Let Render use the repository `Dockerfile`.
3. Deploy the service without adding a separate start command.
4. Optional: set the health check path to `/healthz`.

The container starts the Dialogic Solution dashboard directly and binds to Render's `PORT` automatically.

Notes for hosted use:

- the dashboard download buttons work normally on Render
- local folder reveal is disabled automatically on hosted Linux deployments
- if you want outputs to survive restarts or redeploys, attach a persistent disk in Render and write output folders to that mounted path

## Nigeria Workflow

The Nigeria workflow stays separate and unchanged in structure.

```bash
cd nigeria_profs
python extract_professor_emails.py
python build_professor_contacts_docx.py
```

Nigeria outputs remain in `nigeria_profs/`.

## Generic Country Workflow

Run the generic pipeline by passing the country name:

```bash
python run_country_professor_extraction.py Kenya
```

When the command runs, the script creates an output folder automatically:

- `Kenya` -> `kenya_profs/`
- `Uganda` -> `uganda_profs/`
- `Ghana` -> `ghana_profs/`

The output file names follow the same pattern:

- `<country>_profs/<country>_professor_emails.csv`
- `<country>_profs/<country>_professor_emails.xlsx`

Examples:

```bash
python run_country_professor_extraction.py Kenya
python run_country_professor_extraction.py Uganda
python run_country_professor_extraction.py Ghana
python run_country_professor_extraction.py Rwanda
```

## Recommended First Run For A New Country

When testing a new country, start with a smoke run inside `Test/` so you do not clutter the repo root:

```bash
python run_country_professor_extraction.py Ghana --limit 5 --output-dir Test/ghana_smoke
```

After you confirm the crawl is behaving well, run the full country job:

```bash
python run_country_professor_extraction.py Ghana
```

## Useful Command Options

### Limit Institution Count

Useful for testing:

```bash
python run_country_professor_extraction.py Kenya --limit 5
```

### Target Specific Institutions Only

Use official institution names separated by commas:

```bash
python run_country_professor_extraction.py Kenya --institutions "University of Nairobi,Strathmore University"
python run_country_professor_extraction.py Uganda --institutions "Makerere University,Kyambogo University"
```

`--universities` is also accepted as an alias for `--institutions`.

### Control Crawl Depth

```bash
python run_country_professor_extraction.py Kenya --max-pages 40 --second-pass-pages 30 --workers 4
```

Meaning:

- `--max-pages`: first-pass HTML crawl budget per institution
- `--second-pass-pages`: extra profile or department pages for low-yield institutions
- `--workers`: number of concurrent institution crawlers

### Write Outputs Somewhere Else

```bash
python run_country_professor_extraction.py Kenya --output-dir Test/kenya_debug
```

## Output Files

For a run like:

```bash
python run_country_professor_extraction.py Kenya
```

the script writes:

- `kenya_profs/kenya_professor_emails.csv`
- `kenya_profs/kenya_professor_emails.xlsx`

The Excel workbook contains these sheets:

- `Professor_Emails`: final included rows
- `Coverage_Queue`: per-institution crawl summary
- `Crawl_Log`: page-level crawl/debug records
- `Domains`: discovered or validated institution domains
- `Review_Excluded`: excluded rows and exclusion reasons
- `Method`: extraction method notes
- `Summary`: run totals

There is no separate `scrape_log.json`. The workbook already contains the crawl and review logs.

## What Goes Into `Professor_Emails`

The final sheet keeps only rows that pass the main filters:

- public official source
- valid person-to-email linkage
- relevant lecturer/professor-style rank
- real public email present

Rows with weak linkage, no email, generic contact mailboxes, duplicates, or ambiguous rank are pushed into `Review_Excluded` instead.

## How To Use It For Other Countries

You do not need to create a new country file before testing another African country.

Start with the country name directly:

```bash
python run_country_professor_extraction.py Tanzania
python run_country_professor_extraction.py Zambia
python run_country_professor_extraction.py South_Africa
```

The runner normalizes spaces, hyphens, and underscores in the country name. These all work the same way:

```bash
python run_country_professor_extraction.py "South Africa"
python run_country_professor_extraction.py South-Africa
python run_country_professor_extraction.py South_Africa
```

Recommended process for a new country:

1. run a small smoke test in `Test/`
2. inspect `Coverage_Queue`, `Domains`, and `Review_Excluded` in the workbook
3. if the seed queue looks correct, run the full country job
4. if needed, rerun targeted institutions with `--institutions`

## Practical Notes

- The generic pipeline is currently aimed at African countries.
- It works best where official regulator, ministry, or university pages are publicly accessible.
- Domain discovery is heuristic. Some countries will need more tuning than others.
- Crawl quality depends heavily on how much public staff information institutions actually expose.
- If recall is low, inspect the workbook sheets first before changing code.

## Debugging Low Yield

If a country or institution returns too few rows:

1. check `Coverage_Queue` to see whether pages were actually crawled
2. check `Domains` to see whether official domains were resolved correctly
3. check `Crawl_Log` to see whether staff pages or PDFs were discovered
4. check `Review_Excluded` to see whether valid rows were filtered too aggressively
5. rerun a narrower job with `--institutions` and a larger page budget

Example:

```bash
python run_country_professor_extraction.py Kenya --institutions "University of Nairobi,Jomo Kenyatta University of Agric and Tech" --max-pages 50 --second-pass-pages 40 --workers 1 --output-dir Test/kenya_deep_debug
```

## Rules Followed By The Pipeline

- official sources only
- public pages only
- no logins
- no CAPTCHA bypass
- no fabricated emails
- no personal email invention
- institutional emails preferred

## Summary

Use Nigeria scripts only for the Nigeria legacy workflow.

Use the generic runner for Kenya and other countries:

```bash
python run_country_professor_extraction.py <CountryName>
```

Start new-country tests in `Test/`, review the workbook, then run the full country extraction into `<country>_profs/`.
