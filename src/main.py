import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests
import yaml
from jinja2 import Template


def fetch_url(template: str, **kwargs) -> str:
    url = Template(template).render(**kwargs)
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def run_parallel(tasks):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(task) for task in tasks]
        for future in as_completed(futures):
            future.result()


def schema_dir(group: str, subgroup: str | None = None) -> Path:
    path = Path(f"schemas/{group}")
    if subgroup:
        path = path / subgroup
    return path


def collect_versions(parsed: Any) -> dict[str, dict]:
    return {
        version["name"]: version["schema"]["openAPIV3Schema"]
        for version in parsed["spec"]["versions"]
    }


def write_schema(directory: Path, name: str, schema: dict, version: str):
    directory.mkdir(parents=True, exist_ok=True)
    with open(directory / f"{name}_{version}.json", "w") as f:
        json.dump(schema, f, indent=2)


def store_crd(directory: Path, name: str, parsed: Any):
    for version, data in collect_versions(parsed).items():
        write_schema(directory, name, data, version)


@dataclass
class CRDCollection:
    group: str
    ref: str
    template: str

    def process(self):
        raw = fetch_url(self.template, version=self.ref)
        directory = schema_dir(self.group)
        for parsed in yaml.safe_load_all(raw):
            if parsed and parsed["spec"]["group"] == self.group:
                store_crd(directory, parsed["spec"]["names"]["singular"], parsed)


@dataclass
class CRD:
    group: str
    names: list[str]
    ref: str
    template: str
    subgroup: Optional[str] = None

    def process(self):
        directory = schema_dir(self.group, self.subgroup)

        def process_kind(kind: str):
            raw = fetch_url(self.template, version=self.ref, name=kind)
            crd = yaml.safe_load(raw)
            store_crd(directory, crd["spec"]["names"]["singular"], crd)

        run_parallel([lambda k=kind: process_kind(k) for kind in self.names])


@dataclass
class Config:
    crds: list[CRD]
    collections: list[CRDCollection]

    def process(self):
        run_parallel([item.process for item in self.crds + self.collections])


def load(config: str) -> Config:
    with open(config) as f:
        data = yaml.safe_load(f)
        crds = [CRD(**crd) for crd in data["crds"]]
        collections = [CRDCollection(**crd) for crd in data["collections"]]
        return Config(crds=crds, collections=collections)


def main():
    parser = argparse.ArgumentParser(prog="crds")
    parser.add_argument("-c", "--config", default="crds.yaml")
    args = parser.parse_args()

    config = load(args.config)
    config.process()


if __name__ == "__main__":
    main()
