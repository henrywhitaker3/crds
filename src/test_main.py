import json
from pathlib import Path
from unittest.mock import patch

import yaml

from main import (
    CRD,
    CRDCollection,
    Config,
    collect_versions,
    load,
    schema_dir,
    store_crd,
    write_schema,
)


def make_crd_yaml(group: str, singular: str, versions: dict[str, dict]) -> str:
    return yaml.dump({
        "spec": {
            "group": group,
            "names": {"singular": singular},
            "versions": [
                {"name": name, "schema": {"openAPIV3Schema": schema}}
                for name, schema in versions.items()
            ],
        }
    })


FAKE_SCHEMA_V1 = {"type": "object", "properties": {"foo": {"type": "string"}}}
FAKE_SCHEMA_V2 = {"type": "object", "properties": {"bar": {"type": "integer"}}}


class TestSchemaDir:
    def test_without_subgroup(self):
        assert schema_dir("example.io") == Path("schemas/example.io")

    def test_with_subgroup(self):
        assert schema_dir("example.io", "sub") == Path("schemas/example.io/sub")


class TestCollectVersions:
    def test_extracts_versions(self):
        parsed = yaml.safe_load(make_crd_yaml("g", "thing", {"v1": FAKE_SCHEMA_V1, "v2": FAKE_SCHEMA_V2}))
        versions = collect_versions(parsed)
        assert set(versions.keys()) == {"v1", "v2"}
        assert versions["v1"] == FAKE_SCHEMA_V1
        assert versions["v2"] == FAKE_SCHEMA_V2


class TestWriteSchema:
    def test_creates_file(self, tmp_path):
        write_schema(tmp_path / "out", "widget", FAKE_SCHEMA_V1, "v1")
        result = json.loads((tmp_path / "out" / "widget_v1.json").read_text())
        assert result == FAKE_SCHEMA_V1

    def test_creates_nested_dirs(self, tmp_path):
        write_schema(tmp_path / "a" / "b", "widget", FAKE_SCHEMA_V1, "v1")
        assert (tmp_path / "a" / "b" / "widget_v1.json").exists()


class TestStoreCrd:
    def test_writes_all_versions(self, tmp_path):
        parsed = yaml.safe_load(make_crd_yaml("g", "thing", {"v1": FAKE_SCHEMA_V1, "v2": FAKE_SCHEMA_V2}))
        store_crd(tmp_path, "thing", parsed)
        assert json.loads((tmp_path / "thing_v1.json").read_text()) == FAKE_SCHEMA_V1
        assert json.loads((tmp_path / "thing_v2.json").read_text()) == FAKE_SCHEMA_V2


class TestCRDProcess:
    @patch("main.fetch_url")
    def test_fetches_and_stores(self, mock_fetch, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        crd_yaml = make_crd_yaml("example.io", "widget", {"v1": FAKE_SCHEMA_V1})
        mock_fetch.return_value = crd_yaml

        crd = CRD(group="example.io", names=["widgets"], ref="v1.0.0", template="https://example.com/{{ version }}/{{ name }}.yaml")
        crd.process()

        mock_fetch.assert_called_once_with("https://example.com/{{ version }}/{{ name }}.yaml", version="v1.0.0", name="widgets")
        result = json.loads((tmp_path / "schemas" / "example.io" / "widget_v1.json").read_text())
        assert result == FAKE_SCHEMA_V1

    @patch("main.fetch_url")
    def test_with_subgroup(self, mock_fetch, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        crd_yaml = make_crd_yaml("example.io", "widget", {"v1": FAKE_SCHEMA_V1})
        mock_fetch.return_value = crd_yaml

        crd = CRD(group="example.io", names=["widgets"], ref="v1.0.0", template="t", subgroup="experimental")
        crd.process()

        assert (tmp_path / "schemas" / "example.io" / "experimental" / "widget_v1.json").exists()

    @patch("main.fetch_url")
    def test_multiple_names(self, mock_fetch, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        mock_fetch.side_effect = [
            make_crd_yaml("g", "alpha", {"v1": FAKE_SCHEMA_V1}),
            make_crd_yaml("g", "beta", {"v1": FAKE_SCHEMA_V2}),
        ]

        crd = CRD(group="g", names=["alphas", "betas"], ref="v1.0.0", template="t")
        crd.process()

        assert (tmp_path / "schemas" / "g" / "alpha_v1.json").exists()
        assert (tmp_path / "schemas" / "g" / "beta_v1.json").exists()


class TestCRDCollectionProcess:
    @patch("main.fetch_url")
    def test_filters_by_group(self, mock_fetch, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        docs = "---\n".join([
            make_crd_yaml("target.io", "widget", {"v1": FAKE_SCHEMA_V1}),
            make_crd_yaml("other.io", "gadget", {"v1": FAKE_SCHEMA_V2}),
        ])
        mock_fetch.return_value = docs

        collection = CRDCollection(group="target.io", ref="1.0.0", template="t")
        collection.process()

        assert (tmp_path / "schemas" / "target.io" / "widget_v1.json").exists()
        assert not (tmp_path / "schemas" / "target.io" / "gadget_v1.json").exists()

    @patch("main.fetch_url")
    def test_multiple_matching_docs(self, mock_fetch, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        docs = "---\n".join([
            make_crd_yaml("g", "alpha", {"v1": FAKE_SCHEMA_V1}),
            make_crd_yaml("g", "beta", {"v2": FAKE_SCHEMA_V2}),
        ])
        mock_fetch.return_value = docs

        collection = CRDCollection(group="g", ref="1.0.0", template="t")
        collection.process()

        assert (tmp_path / "schemas" / "g" / "alpha_v1.json").exists()
        assert (tmp_path / "schemas" / "g" / "beta_v2.json").exists()


class TestConfigProcess:
    @patch("main.fetch_url")
    def test_processes_crds_and_collections(self, mock_fetch, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        crd_yaml = make_crd_yaml("a.io", "widget", {"v1": FAKE_SCHEMA_V1})
        collection_yaml = make_crd_yaml("b.io", "gadget", {"v1": FAKE_SCHEMA_V2})

        def fake_fetch(template, **kwargs):
            if "name" in kwargs:
                return crd_yaml
            return collection_yaml

        mock_fetch.side_effect = fake_fetch

        config = Config(
            crds=[CRD(group="a.io", names=["widgets"], ref="v1", template="t")],
            collections=[CRDCollection(group="b.io", ref="v1", template="t")],
        )
        config.process()

        assert (tmp_path / "schemas" / "a.io" / "widget_v1.json").exists()
        assert (tmp_path / "schemas" / "b.io" / "gadget_v1.json").exists()


class TestLoad:
    def test_loads_config(self, tmp_path):
        config_data = {
            "crds": [{"group": "a.io", "names": ["widgets"], "ref": "v1", "template": "t"}],
            "collections": [{"group": "b.io", "ref": "v1", "template": "t"}],
        }
        config_file = tmp_path / "crds.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = load(str(config_file))

        assert len(config.crds) == 1
        assert config.crds[0].group == "a.io"
        assert config.crds[0].names == ["widgets"]
        assert len(config.collections) == 1
        assert config.collections[0].group == "b.io"
