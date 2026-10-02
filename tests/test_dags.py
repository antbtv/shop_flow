"""DAG import test inside the Airflow image (Airflow is not installed on the laptop).

Parses airflow/dags with the image's Airflow, as the dag-processor on Pi5 does. Skipped when
Docker or the image is missing; build it with: docker build -t shopflow-airflow:3.1.0 airflow/
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

IMAGE = "shopflow-airflow:3.1.0"
DAGS = Path(__file__).resolve().parent.parent / "airflow" / "dags"
EXPECTED_DAGS = {
    "shopflow_healthcheck",
    "shopflow_reconciliation",
    "shopflow_data_quality",
    "shopflow_retention",
}

# PYTHONPATH as in docker-compose.pi5.yml: DAG files import shopflow_common;
# .airflowignore keeps the helpers out of parsing.
PROBE = """
import json
from airflow.models.dagbag import DagBag
bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
errors = {k: str(v)[-500:] for k, v in bag.import_errors.items()}
print("RESULT " + json.dumps({"errors": errors, "dags": sorted(bag.dag_ids)}))
"""


def _image_present() -> bool:
    if not shutil.which("docker"):
        return False
    probe = subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True)
    return probe.returncode == 0


@pytest.fixture(scope="module")
def dagbag() -> dict:
    if not _image_present():
        pytest.skip(f"docker or image {IMAGE} not available")
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--entrypoint", "python",
         "-e", "AIRFLOW__CORE__LOAD_EXAMPLES=False", "-e", "PYTHONPATH=/opt/airflow/dags",
         "-v", f"{DAGS}:/opt/airflow/dags:ro", IMAGE, "-c", PROBE],
        capture_output=True, text=True, timeout=180, check=True,
    ).stdout
    line = next(x for x in out.splitlines() if x.startswith("RESULT "))
    return json.loads(line.removeprefix("RESULT "))


def test_no_import_errors(dagbag):
    assert dagbag["errors"] == {}


def test_expected_dags_present(dagbag):
    assert EXPECTED_DAGS <= set(dagbag["dags"])
