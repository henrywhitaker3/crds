// Package processor
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/henrywhitaker3/crds/internal/config"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

type processed struct {
	items []*CRD
	mu    *sync.Mutex
}

func (p *processed) push(c *CRD) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = append(p.items, c)
}

func Process(ctx context.Context, crds []config.CRD, colls []config.Collection) error {
	proc := &processed{
		items: []*CRD{},
		mu:    &sync.Mutex{},
	}

	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.Go(func() error {
		return processCollections(ctx, colls, proc)
	})
	errGrp.Go(func() error {
		return processCRDs(ctx, crds, proc)
	})
	if err := errGrp.Wait(); err != nil {
		return err
	}

	slog.Info("finished processing crds", "count", len(proc.items))
	orphans, _ := findOrphans("schemas", proc)
	if len(orphans) > 0 {
		slog.Warn("found orphaned crds", "count", len(orphans), "orphans", orphans)
	}

	return nil
}

func processCollections(ctx context.Context, colls []config.Collection, proc *processed) error {
	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.SetLimit(runtime.NumCPU() * 2)

	out := []error{}

	for _, c := range colls {
		slog := slog.With("collection", c)
		slog.Debug("processing collection")
		errGrp.Go(func() error {
			crds, err := ProcessCollection(ctx, c)
			if err != nil {
				out = append(out, err)
				slog.Error("process collection", "error", err)
				return nil
			}
			for _, crd := range crds {
				if err := crd.Write(); err != nil {
					out = append(out, err)
					slog.Error("write crd", "error", err)
					return nil
				}
				proc.push(crd)
			}
			return nil
		})
	}

	if err := errGrp.Wait(); err != nil {
		return err
	}

	if len(out) > 0 {
		return errors.Join(out...)
	}

	return nil
}

func processCRDs(ctx context.Context, crds []config.CRD, proc *processed) error {
	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.SetLimit(runtime.NumCPU() * 2)

	outErr := []error{}

	for _, c := range crds {
		slog := slog.With("crd", c)
		slog.Debug("processing crd")
		out, err := ProcessCRD(ctx, c)
		if err != nil {
			outErr = append(outErr, err)
			slog.Error("process crd", "error", err)
			continue
		}
		for _, crd := range out {
			if err := crd.Write(); err != nil {
				slog.Error("write crd", "error", err)
				outErr = append(outErr, err)
				continue
			}
			proc.push(crd)
		}
	}

	if err := errGrp.Wait(); err != nil {
		return err
	}

	if len(outErr) > 0 {
		return errors.Join(outErr...)
	}

	return nil
}

type CRD struct {
	Group   string
	Kind    string
	Version string
	Parent  *string
	Schema  map[string]any
}

func (c *CRD) Path() string {
	nameParts := []string{"schemas"}
	if c.Parent != nil {
		nameParts = append(nameParts, *c.Parent)
	}
	nameParts = append(nameParts, c.Group, fmt.Sprintf("%s_%s.json", c.Kind, c.Version))
	return filepath.Join(nameParts...)
}

func (c *CRD) Write() error {
	name := c.Path()
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return fmt.Errorf("ensure directory exists: %w", err)
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(c.Schema); err != nil {
		return fmt.Errorf("marshal schema %s: %w", name, err)
	}

	return nil
}

var (
	cache   = map[string][]map[string]any{}
	cacheMu = &sync.RWMutex{}
	dedupe  = &singleflight.Group{}
)

// Get fetches content from a URL and returns parsed crds from it.
// It will return separate CRDs if there are multiple documents in
// the response body, and strip helm tags from the content too.
func Get(ctx context.Context, url string) ([]map[string]any, error) {
	out, err, _ := dedupe.Do(url, func() (any, error) {
		return get(ctx, url)
	})
	return out.([]map[string]any), err
}

func get(ctx context.Context, url string) ([]map[string]any, error) {
	cacheMu.RLock()
	if val, ok := cache[url]; ok {
		cacheMu.RUnlock()
		return val, nil
	}
	cacheMu.RUnlock()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request %s: %w", url, err)
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request %s: %w", url, err)
	}
	if resp.StatusCode > 299 {
		return nil, fmt.Errorf("got status %s: %d", url, resp.StatusCode)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	decoder := yaml.NewDecoder(strings.NewReader(stripHelmTemplates(string(body))))
	docs := []map[string]any{}
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode yaml document %s: %w", url, err)
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()
	cache[url] = docs

	return docs, nil
}

var helmTemplateRe = regexp.MustCompile(`\{\{.*?\}\}`)

func stripHelmTemplates(raw string) string {
	lines := strings.Split(raw, "\n")
	out := lines[:0]
	for _, line := range lines {
		if !helmTemplateRe.MatchString(line) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

func Template(tmpl string, vars map[string]string) string {
	for k, v := range vars {
		tmpl = strings.ReplaceAll(tmpl, "{{ "+k+" }}", v)
		tmpl = strings.ReplaceAll(tmpl, "{{"+k+"}}", v)
	}
	return tmpl
}

func nestedValue[T any](m map[string]any, keys ...string) (T, bool) {
	var empty T
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return empty, false
		}
		cur, ok = mm[k]
		if !ok {
			return empty, false
		}
	}
	s, ok := cur.(T)
	return s, ok
}

func collectDocVersions(doc map[string]any) (map[string]map[string]any, error) {
	spec, ok := doc["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("doc has no spec")
	}

	versions, ok := spec["versions"].([]any)
	if !ok {
		return nil, fmt.Errorf("spec has no versions")
	}

	out := map[string]map[string]any{}
	for _, ver := range versions {
		version, ok := ver.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("version is not a valid format")
		}
		name, ok := version["name"].(string)
		if !ok {
			return nil, fmt.Errorf("version has no name")
		}

		schema, ok := version["schema"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("version has no schema")
		}

		spec, ok := schema["openAPIV3Schema"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("schema has no openAPIV3Schema")
		}

		out[name] = spec
	}

	return out, nil
}
