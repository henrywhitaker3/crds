package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/henrywhitaker3/crds/internal/config"
	"github.com/henrywhitaker3/crds/internal/processor"
	"golang.org/x/sync/errgroup"
)

func main() {
	conf, err := config.LoadConfig("crds.yaml")
	die(err)

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

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
	errGrp.SetLimit(runtime.NumCPU() * 2)

	for _, c := range colls {
		slog.Debug("processing collection", "collection", c)
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
	errGrp.SetLimit(runtime.NumCPU() * 2)

	for _, c := range crds {
		slog.Debug("processing crd", "crd", c)
		out, err := processor.ProcessCRD(ctx, c)
		if err != nil {
			return err
		}
		for _, crd := range out {
			if err := crd.Write(); err != nil {
				return fmt.Errorf("write crd: %w", err)
			}
		}
	}

	return errGrp.Wait()
}

func die(err error) {
	if err != nil {
		panic(err)
	}
}
