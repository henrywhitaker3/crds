# CRD schemas for YAML editors

This repository publishes JSON Schemas generated from Kubernetes Custom Resource
Definitions (CRDs). Use them with a YAML language server to get completion,
validation, hover documentation, and diagnostics while editing Kubernetes
manifests.

The schema catalogue lives in [`schemas/`](schemas). Each schema is named after
the resource kind and API version:

```text
schemas/<group>/<kind>_<version>.json
```

Some providers group their schemas beneath an additional directory:

```text
schemas/<collection>/<group>/<kind>_<version>.json
```

For example, the `cert-manager.io/v1` `Certificate` schema is:

```text
https://raw.githubusercontent.com/henrywhitaker3/crds/main/schemas/cert-manager.io/certificate_v1.json
```

## Use a schema hint in a manifest

With the [YAML language server](https://github.com/redhat-developer/yaml-language-server)
(including VS Code's YAML extension), put a `yaml-language-server` directive on
the first line of the manifest. The language server downloads the schema and
uses it for that file.

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/henrywhitaker3/crds/main/schemas/cert-manager.io/certificate_v1.json
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: example
spec:
  secretName: example-tls
  issuerRef:
    name: letsencrypt
    kind: ClusterIssuer
  dnsNames:
    - example.com
```

Choose the schema path that matches the resource's `apiVersion` and `kind`.
Schema filenames use lowercase singular kind names, followed by an underscore
and the version. For example:

| Resource | Schema URL |
| --- | --- |
| `cert-manager.io/v1`, `Certificate` | `.../schemas/cert-manager.io/certificate_v1.json` |
| `monitoring.coreos.com/v1`, `ServiceMonitor` | `.../schemas/monitoring.coreos.com/servicemonitor_v1.json` |
| `gateway.networking.k8s.io/v1`, `HTTPRoute` (standard) | `.../schemas/gateway.networking.k8s.io/standard/httproute_v1.json` |
| `networking.istio.io/v1`, `VirtualService` | `.../schemas/istio.io/networking.istio.io/virtualservice_v1.json` |

Replace `...` with:

```text
https://raw.githubusercontent.com/henrywhitaker3/crds/main
```

For a local checkout, the same directive can point to a local schema file:

```yaml
# yaml-language-server: $schema=./schemas/cert-manager.io/certificate_v1.json
```

The relative path is resolved from the YAML file. Use an absolute `file://` URL
instead if the manifest is elsewhere on disk.

## Associate schemas by file pattern

If your manifests follow predictable filenames, configure the YAML extension
once rather than adding a hint to every file. In VS Code, add this to workspace
or user `settings.json`:

```json
{
  "yaml.schemas": {
    "https://raw.githubusercontent.com/henrywhitaker3/crds/main/schemas/cert-manager.io/certificate_v1.json": [
      "certificates/*.yaml"
    ],
    "https://raw.githubusercontent.com/henrywhitaker3/crds/main/schemas/monitoring.coreos.com/servicemonitor_v1.json": [
      "monitoring/service-monitor.yaml"
    ]
  }
}
```

An inline schema hint takes precedence, so it is useful when one directory
contains several resource kinds.

## Updating the catalogue

Schema sources and versions are declared in [`crds.yaml`](crds.yaml). Generate
the catalogue with Go or [mise](https://mise.jdx.dev/):

```sh
go run main.go
# or
mise run generate
```

To remove generated schema files that are no longer present in the
configuration:

```sh
go run main.go --prune
# or
mise run prune
```

Generated schemas are committed to this repository so editors can consume them
directly over HTTPS; regenerating requires network access to the upstream CRD
sources.
