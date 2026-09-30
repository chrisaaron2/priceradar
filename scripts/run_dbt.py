"""
Run dbt with the same settings as the rest of the project.

dbt only reads real environment variables, not .env, and needs the path to the
service-account key. This loads .env, finds the key (include/gcp-key.json by default)
and then runs `dbt deps` followed by the dbt command you pass (default: build).

Usage:
    python scripts/run_dbt.py              # dbt deps + dbt build
    python scripts/run_dbt.py test         # dbt deps + dbt test
    python scripts/run_dbt.py run -s dim_product
"""

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from common.warehouse import use_credentials  # noqa: E402

DBT_DIR = ROOT / "dbt" / "priceradar"


def main() -> int:
    load_dotenv(ROOT / ".env")
    if not use_credentials():
        print("No GCP key found. Save your service-account key as include/gcp-key.json.")
        return 1
    dbt = shutil.which("dbt")
    if not dbt:
        print("dbt is not installed in this environment: pip install dbt-bigquery")
        return 1

    args = sys.argv[1:] or ["build"]
    for command in (["deps"], args):
        result = subprocess.run([dbt, *command, "--profiles-dir", "."], cwd=DBT_DIR)
        if result.returncode != 0:
            return result.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
