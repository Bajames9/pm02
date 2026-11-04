

"""Package Canopy pipeline outputs for download from Databricks.

Creates a ZIP archive containing the source CSV, five Parquet
dataset directories, and the JSON execution report.

The ZIP is an intermediate transfer archive, NOT the final
PM02 submission TAR.

Students must remove the original CSV, manifest files, and
other unnecessary files when assembling their final submission.
"""

import os
import shutil
import zipfile


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

BASE = "/Volumes/workspace/default/de_course_data"

ZIP_NAME = "canopy_reference_data.zip"
ZIP_PATH = f"/tmp/{ZIP_NAME}"
DESTINATION = f"{BASE}/{ZIP_NAME}"

ITEMS = [
    "canopy_project_data.csv",
    "pipeline_report.json",
    "canopy_ingested",
    "canopy_cleaned",
    "canopy_valid",
    "canopy_rejected",
    "canopy_prepared",
]


# ------------------------------------------------------------
# Verify required inputs
# ------------------------------------------------------------

for item in ITEMS:
    path = os.path.join(BASE, item)

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Required pipeline file or directory is missing: {path}"
        )

    if os.path.isdir(path) and not os.listdir(path):
        raise ValueError(
            f"Required dataset directory is empty: {path}"
        )


# ------------------------------------------------------------
# Create ZIP archive
# ------------------------------------------------------------

with zipfile.ZipFile(
    ZIP_PATH,
    mode="w",
    compression=zipfile.ZIP_DEFLATED,
) as archive:

    for item in ITEMS:
        path = os.path.join(BASE, item)

        if os.path.isfile(path):
            archive.write(path, arcname=item)

        elif os.path.isdir(path):
            for root, _, files in os.walk(path):
                for filename in files:
                    full_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(full_path, BASE)

                    archive.write(
                        full_path,
                        arcname=relative_path,
                    )


# ------------------------------------------------------------
# Verify ZIP archive
# ------------------------------------------------------------

with zipfile.ZipFile(ZIP_PATH, "r") as archive:
    bad_file = archive.testzip()

    if bad_file is not None:
        raise RuntimeError(
            f"ZIP verification failed for: {bad_file}"
        )

print(f"ZIP size: {os.path.getsize(ZIP_PATH):,} bytes")


# ------------------------------------------------------------
# Copy ZIP to Unity Catalog volume
# ------------------------------------------------------------

shutil.copyfile(ZIP_PATH, DESTINATION)

print(f"PASS: Created {DESTINATION}")
print("Download the ZIP from the Databricks volume.")
print(
    "This is a transfer archive, not the final PM02 submission."
)

