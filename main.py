import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import requests
import yaml
from jinja2 import Template


@dataclass
class CRDCollection:
    group: str
    ref: str
    template: str

    def fetch(self: CRDCollection) -> str:
        url = Template(self.template).render(version=self.ref)
        response = requests.get(url)
        response.raise_for_status()
        return response.text

    def split(self: CRDCollection, raw: str) -> List[str]:
        return raw.split("---")

    def process(self: CRDCollection):
        crds = self.split(self.fetch())
        for crd in crds:
            parsed = yaml.safe_load(crd)
            if parsed["spec"]["group"] == self.group:
                obj = CRD(
                    group=self.group, names=[], ref=self.ref, template=self.template
                )
                obj.makeGroupDir()
                obj.store(parsed["spec"]["names"]["singular"], parsed)
        pass


@dataclass
class CRD:
    group: str
    names: List[str]
    ref: str
    template: str
    subgroup: Optional[str] = None

    def makeGroupDir(self: CRD):
        Path(f"schemas/{self.group}").mkdir(parents=True, exist_ok=True)
        if self.subgroup:
            Path(f"schemas/{self.group}/{self.subgroup}").mkdir(
                parents=True, exist_ok=True
            )

    def fetch(self: CRD, name: str) -> str:
        url = Template(self.template).render(version=self.ref, name=name)
        response = requests.get(url)
        response.raise_for_status()
        return response.text

    def write(self: CRD, name: str, schema: dict, version: str):
        prefix = f"schemas/{self.group}"
        if self.subgroup:
            prefix = f"{prefix}/{self.subgroup}"
        with open(f"{prefix}/{name}_{version}.json", "w") as f:
            json.dump(schema, f, indent=2)

    def collectVersions(self: CRD, parsed: Any) -> dict[str, dict]:
        return {
            version["name"]: version["schema"]["openAPIV3Schema"]
            for version in parsed["spec"]["versions"]
        }

    def store(self: CRD, name: str, parsed: Any):
        versions = self.collectVersions(parsed)
        for version, data in versions.items():
            self.write(name, data, version)

    def processKind(self: CRD, kind: str):
        crd = yaml.safe_load(self.fetch(kind))
        self.store(crd["spec"]["names"]["singular"], crd)

    def process(self: CRD):
        self.makeGroupDir()
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.processKind, kind) for kind in self.names]
            for future in as_completed(futures):
                future.result()


@dataclass
class Config:
    crds: List[CRD]
    collections: List[CRDCollection]

    def processCRDs(self: Config):
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(crd.process) for crd in self.crds]
            for future in as_completed(futures):
                future.result()

    def processCollections(self: Config):
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(crd.process) for crd in self.collections]
            for future in as_completed(futures):
                future.result()


def load(config: str) -> Config:
    with open(config) as f:
        data = yaml.safe_load(f)
        crds = [CRD(**crd) for crd in data["crds"]]
        collections = [CRDCollection(**crd) for crd in data["collections"]]
        return Config(crds=crds, collections=collections)


def parse(crd: str) -> CRD:
    data = yaml.safe_load(crd)
    return CRD(**data)


def main():
    parser = argparse.ArgumentParser(prog="crds")
    parser.add_argument("-c", "--config", default="crds.yaml")
    args = parser.parse_args()

    config = load(args.config)
    config.processCRDs()
    config.processCollections()


if __name__ == "__main__":
    main()
