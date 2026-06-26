package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/henrywhitaker3/crds/internal/config"
	"github.com/henrywhitaker3/crds/internal/processor"
	"golang.org/x/sync/errgroup"
)

func main() {
	conf, err := config.LoadConfig("crds.yaml")
	die(err)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.Go(func() error {
		return processCollections(ctx, conf.Collections)
	})
	errGrp.Go(func() error {
		return processCRDs(ctx, conf.CRDs)
	})

	die(errGrp.Wait())
}

var ()

func processCollections(ctx context.Context, colls []config.Collection) error {
	errGrp, ctx := errgroup.WithContext(ctx)

	for _, c := range colls {
		errGrp.Go(func() error {
			crds, err := processor.ProcessCollection(ctx, c)
			if err != nil {
				return fmt.Errorf("process coll %s: %w", c.Group, err)
			}
			for _, crd := range crds {
				if err := crd.Write(); err != nil {
					return fmt.Errorf("write crd: %w", err)
				}
			}
			return nil
		})
	}

	return errGrp.Wait()
}

func processCRDs(ctx context.Context, crds []config.CRD) error {
	errGrp, ctx := errgroup.WithContext(ctx)

	for _, c := range crds {
		out, err := processor.ProcessCRD(ctx, c)
		if err != nil {
			return err
		}
		for _, crd := range out {
			if err := crd.Write(); err != nil {
				return fmt.Errorf("write crd: %w", err)
			}
		}
		return nil
	}

	return errGrp.Wait()
}

func die(err error) {
	if err != nil {
		panic(err)
	}
}
