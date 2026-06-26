package processor

import (
	"context"
	"fmt"

	"github.com/henrywhitaker3/crds/internal/config"
)

func ProcessCollection(ctx context.Context, coll config.Collection) ([]*CRD, error) {
	docs, err := Get(ctx, Template(coll.Template, map[string]string{"version": coll.Ref}))
	if err != nil {
		return nil, fmt.Errorf("get raw docs: %w", err)
	}

	out := []*CRD{}
	for _, doc := range docs {
		group, ok := nestedValue[string](doc, "spec", "group")
		if !ok || coll.Group != group {
			continue
		}
		versions, err := collectDocVersions(doc)
		if err != nil {
			return nil, fmt.Errorf("parse doc versions: %w", err)
		}

		name, ok := nestedValue[string](doc, "spec", "names", "singular")
		if !ok {
			return nil, fmt.Errorf("find singular name")
		}

		for v, spec := range versions {
			out = append(out, &CRD{
				Group:   group,
				Kind:    name,
				Version: v,
				Parent:  coll.Parent,
				Schema:  spec,
			})
		}
	}

	return out, nil
}
