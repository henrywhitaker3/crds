package processor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/henrywhitaker3/crds/internal/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func mockHTTPClient(t *testing.T, transport roundTripperFunc) {
	t.Helper()

	originalClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: transport}

	cacheMu.Lock()
	originalCache := cache
	cache = map[string][]map[string]any{}
	cacheMu.Unlock()
	originalDedupe := dedupe
	dedupe = &singleflight.Group{}

	t.Cleanup(func() {
		http.DefaultClient = originalClient
		cacheMu.Lock()
		cache = originalCache
		cacheMu.Unlock()
		dedupe = originalDedupe
	})
}

func mockResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestGetFetchesParsesAndCachesDocuments(t *testing.T) {
	var requests int
	mockHTTPClient(t, func(request *http.Request) (*http.Response, error) {
		requests++
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "https://example.test/crds.yaml", request.URL.String())
		return mockResponse(http.StatusOK, `
apiVersion: example.io/v1
kind: Widget
metadata:
  name: {{ .Values.name }}
---
apiVersion: example.io/v1
kind: Gadget
`), nil
	})

	docs, err := Get(context.Background(), "https://example.test/crds.yaml")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{
		{"apiVersion": "example.io/v1", "kind": "Widget", "metadata": nil},
		{"apiVersion": "example.io/v1", "kind": "Gadget"},
	}, docs)

	cachedDocs, err := Get(context.Background(), "https://example.test/crds.yaml")
	require.NoError(t, err)
	require.Equal(t, docs, cachedDocs)
	require.Equal(t, 1, requests)
}

func TestGetReturnsFetchAndHTTPStatusErrors(t *testing.T) {
	mockHTTPClient(t, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network unavailable")
	})

	docs, err := Get(context.Background(), "https://example.test/unavailable.yaml")
	require.Nil(t, docs)
	require.ErrorContains(t, err, "do request")

	mockHTTPClient(t, func(*http.Request) (*http.Response, error) {
		return mockResponse(http.StatusBadGateway, "upstream failed"), nil
	})

	docs, err = Get(context.Background(), "https://example.test/failed.yaml")
	require.Nil(t, docs)
	require.ErrorContains(t, err, "got status https://example.test/failed.yaml: 502")
}

func TestProcessCRDUsesTemplateAndBuildsSchemas(t *testing.T) {
	var requestedURL string
	mockHTTPClient(t, func(request *http.Request) (*http.Response, error) {
		requestedURL = request.URL.String()
		return mockResponse(http.StatusOK, `
spec:
  names:
    singular: widget
  versions:
    - name: v1
      schema:
        openAPIV3Schema:
          type: object
`), nil
	})

	subgroup := "apps.example.io"
	crds, err := ProcessCRD(context.Background(), config.CRD{
		Group:    "example.io",
		Subgroup: &subgroup,
		Ref:      "v1.2.3",
		Template: "https://example.test/{{ version }}/{{name}}.yaml",
		Names:    []string{"Widget"},
	})

	require.NoError(t, err)
	require.Equal(t, "https://example.test/v1.2.3/Widget.yaml", requestedURL)
	require.Equal(t, []*CRD{{
		Group:   "apps.example.io",
		Kind:    "widget",
		Version: "v1",
		Parent:  new("example.io"),
		Schema:  map[string]any{"type": "object"},
	}}, crds)
}

func TestProcessCollectionFiltersOtherGroups(t *testing.T) {
	mockHTTPClient(t, func(*http.Request) (*http.Response, error) {
		return mockResponse(http.StatusOK, `
spec:
  group: ignored.example.io
  names:
    singular: ignored
  versions: []
---
spec:
  group: example.io
  names:
    singular: widget
  versions:
    - name: v1beta1
      schema:
        openAPIV3Schema:
          type: object
`), nil
	})

	parent := "collection"
	crds, err := ProcessCollection(context.Background(), config.Collection{
		Group:    "example.io",
		Parent:   &parent,
		Ref:      "v1.2.3",
		Template: "https://example.test/{{version}}/all.yaml",
	})

	require.NoError(t, err)
	require.Equal(t, []*CRD{{
		Group:   "example.io",
		Kind:    "widget",
		Version: "v1beta1",
		Parent:  new("collection"),
		Schema:  map[string]any{"type": "object"},
	}}, crds)
}
