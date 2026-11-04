"""The DAG tests run inside the Airflow image."""

import importlib.util

# The repository's airflow/ folder also resolves as `airflow`, hence the submodule.
collect_ignore_glob = [] if importlib.util.find_spec("airflow.sdk") else ["test_*.py"]
