package processor

import (
	"context"
	"fmt"

	"github.com/henrywhitaker3/crds/internal/config"
)

func ProcessCRD(ctx context.Context, crd config.CRD) ([]*CRD, error) {
	out := []*CRD{}

	for _, kind := range crd.Names {
		crds, err := processCRDKind(ctx, crd, kind)
		if err != nil {
			return nil, fmt.Errorf("process crd kind: %w", err)
		}
		out = append(out, crds...)
	}

	return out, nil
}

func processCRDKind(ctx context.Context, crd config.CRD, kind string) ([]*CRD, error) {
	docs, err := Get(
		ctx,
		Template(crd.Template, map[string]string{"version": crd.Ref, "name": kind}),
	)
	if err != nil {
		return nil, fmt.Errorf("get raw docs: %w", err)
	}

	out := []*CRD{}

	for _, doc := range docs {
		versions, err := collectDocVersions(doc)
		if err != nil {
			return nil, fmt.Errorf("parse doc versions: %w", err)
		}

		name, ok := nestedValue[string](doc, "spec", "names", "singular")
		if !ok {
			return nil, fmt.Errorf("find singular name")
		}

		for v, spec := range versions {
			group := crd.Group
			var parent *string
			if crd.Subgroup != nil {
				parent = &crd.Group
				group = *crd.Subgroup
			}

			out = append(out, &CRD{
				Group:   group,
				Kind:    name,
				Version: v,
				Parent:  parent,
				Schema:  spec,
			})
		}
	}

	return out, nil
}
