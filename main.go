package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/henrywhitaker3/crds/internal/config"
	"github.com/henrywhitaker3/crds/internal/processor"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

var (
	logLevel   string = "info"
	configPath string = "crds.yaml"
)

func main() {
	flags := pflag.NewFlagSet("flags", pflag.ExitOnError)
	flags.StringVar(&logLevel, "log-level", "info", "The log verbosity")
	flags.StringVarP(&configPath, "config", "c", "crds.yaml", "The path to the config file")

	if err := flags.Parse(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	conf, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slogLevel(logLevel),
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

	if err := errGrp.Wait(); err != nil {
		slog.Error("failed processing", "error", err)
		os.Exit(1)
	}
}

var ()

func processCollections(ctx context.Context, colls []config.Collection) error {
	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.SetLimit(runtime.NumCPU() * 2)

	out := []error{}

	for _, c := range colls {
		slog := slog.With("collection", c)
		slog.Debug("processing collection")
		errGrp.Go(func() error {
			crds, err := processor.ProcessCollection(ctx, c)
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

func processCRDs(ctx context.Context, crds []config.CRD) error {
	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.SetLimit(runtime.NumCPU() * 2)

	outErr := []error{}

	for _, c := range crds {
		slog := slog.With("crd", c)
		slog.Debug("processing crd")
		out, err := processor.ProcessCRD(ctx, c)
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

func slogLevel(level string) slog.Level {
	switch level {
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	case "debug":
		return slog.LevelDebug
	case "info":
		fallthrough
	default:
		return slog.LevelInfo
	}
}
