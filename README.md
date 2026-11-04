# PM02 — Canopy Data Pipeline

Starter package for PM02: a PySpark pipeline, run in Databricks, that ingests, cleans, validates, and transforms Canopy order data and writes five Parquet datasets plus a JSON execution report. You implement four modules; the rest of the application is provided.

## Documents

| Document | Use it for |
|---|---|
| Engineering Design Specification | Processing rules, schemas, validation codes, calculations (authoritative for behavior) |
| PM02 Implementation Assignment | Responsibilities, verification evidence, grading, submission |
| This README | Setup, running the application, packaging outputs |

## Contents

```
pm02_start/
|-- README.md
|-- data/
|   +-- canopy_project_data.csv        # 50,000 records, 25 columns; do not modify
|-- notebooks/
|   +-- pm02_development_notebook.ipynb  # experimentation and verification
|-- src/
|   |-- ingest.py       # student implements
|   |-- clean.py        # student implements
|   |-- validate.py     # student implements
|   |-- transform.py    # student implements
|   |-- output.py       # provided; do not modify
|   |-- report.py       # provided; do not modify
|   +-- main.py         # provided; edit only the path configuration
+-- tools/
    +-- package_output.py  # copies pipeline outputs into a downloadable ZIP
```

Keep the supplied function names, parameters, and return values. Helper functions are allowed. Module docstrings describe each function's interface.

## Setup

Use Databricks with Serverless compute.

1. **Upload the modules.** Create a Workspace folder (for example, `PM02_Canopy_Pipeline`) and upload all seven `.py` files from `src/` into it. Keep them together so `main.py` can import the others.
2. **Import the notebook.** Import `pm02_development_notebook.ipynb` into your Workspace.
3. **Upload the CSV.** Upload `canopy_project_data.csv` to a Unity Catalog volume. The application expects:

   ```text
   /Volumes/workspace/default/de_course_data/canopy_project_data.csv
   ```

   Use this volume unless your instructor says otherwise. If you use a different one, update `main.py` **and** `BASE` in `package_output.py` so they match:

   ```python
   DATA_PATH = "/Volumes/workspace/default/de_course_data"
   INPUT_PATH = f"{DATA_PATH}/canopy_project_data.csv"
   OUTPUT_PATH = DATA_PATH
   ```

4. **Verify the environment.** Run the setup cell in the notebook and confirm Spark can read the CSV (50,000 records, 25 columns).

## Running the Pipeline

From a notebook that can import your Workspace folder:

```python
from main import main

main()
```

`main()` gets the active Spark session and runs every stage in order: ingest, clean, validate, transform, write and verify outputs, then write the report. You do not call individual stages yourself.

A successful run writes the following to the volume and verifies the written datasets (record counts, schemas, and stage-to-stage reconciliation):

```
de_course_data/
|-- canopy_project_data.csv
|-- canopy_ingested/
|-- canopy_cleaned/
|-- canopy_valid/
|-- canopy_rejected/
|-- canopy_prepared/
+-- pipeline_report.json
```

The processing logic must live in the `.py` modules, not only in notebook cells. Use the notebook to prototype and test before running the full application.

## Packaging Outputs

`tools/package_output.py` bundles the pipeline outputs so you can download them from Databricks.

1. Run the pipeline successfully first.
2. Upload `package_output.py` to your Workspace and run it in Databricks.
3. The utility checks that the required files exist, creates `canopy_reference_data.zip` in the volume, and verifies the archive.
4. Download the ZIP from the volume and extract it on your computer.

The ZIP contains the source CSV, the five Parquet directories, and `pipeline_report.json`. It is a transfer file only, not your submission. See the assignment for what to submit and how to assemble it.

## Troubleshooting

| Problem | Fix |
|---|---|
| `main` or another module cannot be imported | Confirm all seven modules are in the same Workspace folder and that folder is on the notebook's Python import path. |
| Source CSV not found | Check the volume location and `DATA_PATH` / `INPUT_PATH` in `main.py`. |
| Results look wrong | Compare the function against the Design Specification, then test sample records in the notebook before rerunning. |
| Failure during output verification | Read the error message; it reports schema or count mismatches for the written datasets. |
| Packaging utility reports a missing file | Confirm the pipeline completed and all five datasets and the report exist in the volume. Check that `BASE` matches `main.py`. |

## Team Execution Notes

*Complete before submitting.*

**How we ran the pipeline:** describe how your team ran the application in Databricks, including any configuration changes.

**Known limitations or unresolved issues:** describe remaining problems, or state that none are known.
