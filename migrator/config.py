"""Settings from env vars, plus the mapping.yaml loader."""
import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    mongo_uri: str
    mongo_db: str
    kafka_bootstrap: str
    connect_url: str


def get_settings() -> Settings:
    return Settings(
        pg_dsn=os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5433/bank"),
        mongo_uri=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.environ.get("MONGO_DB", "bank"),
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        connect_url=os.environ.get("CONNECT_URL", "http://localhost:8083"),
    )


def load_mapping(path: str | Path = "mapping.yaml") -> dict:
    with open(path) as f:
        mapping = yaml.safe_load(f)
    validate_mapping(mapping)
    return mapping


def validate_mapping(m: dict) -> None:
    if not isinstance(m, dict) or "collections" not in m:
        raise ValueError("mapping.yaml must contain a 'collections' section")
    for name, c in m["collections"].items():
        for key in ("from_table", "id"):
            if key not in c:
                raise ValueError(f"collection {name!r} is missing {key!r}")
        for e in c.get("embed", []):
            for key in ("table", "as", "fk", "id"):
                if key not in e:
                    raise ValueError(f"{name}.embed entry is missing {key!r}")
        for r in c.get("extended_refs", []):
            for key in ("table", "as", "fk", "fields"):
                if key not in r:
                    raise ValueError(f"{name}.extended_refs entry is missing {key!r}")
        for mm in c.get("many_to_many", []):
            for key in ("via", "as", "local_fk", "remote_fk"):
                if key not in mm:
                    raise ValueError(f"{name}.many_to_many entry is missing {key!r}")
