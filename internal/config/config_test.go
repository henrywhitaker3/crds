package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "crds.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`
crds:
  - group: example.io
    subgroup: apps.example.io
    ref: v1.2.3
    template: https://example.com/{{ version }}/{{ name }}.yaml
    names: [widget]
collections:
  - group: example.io
    parent: collection
    ref: v2.0.0
    template: https://example.com/{{ version }}/all.yaml
`), 0o600))

	config, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.Len(t, config.CRDs, 1)
	require.Equal(t, "example.io", config.CRDs[0].Group)
	require.Equal(t, "apps.example.io", *config.CRDs[0].Subgroup)
	require.Equal(t, []string{"widget"}, config.CRDs[0].Names)
	require.Len(t, config.Collections, 1)
	require.Equal(t, "collection", *config.Collections[0].Parent)
}

func TestLoadConfigReturnsErrorsForUnreadableOrInvalidFiles(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	require.ErrorContains(t, err, "read config file")

	configPath := filepath.Join(t.TempDir(), "invalid.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("crds: ["), 0o600))

	_, err = LoadConfig(configPath)
	require.ErrorContains(t, err, "unmarshal config")
}
