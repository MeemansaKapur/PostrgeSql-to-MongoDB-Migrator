from pathlib import Path

import pytest
import yaml

from migrator.transform.mapping import Mapping

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def mapping() -> Mapping:
    return Mapping.from_dict(yaml.safe_load((ROOT / "mapping.yaml").read_text()))
