"""Põe `dags/` no path: no Airflow ela é a pasta das DAGs, aqui é só um pacote."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
