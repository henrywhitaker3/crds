package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/henrywhitaker3/crds/internal/config"
	"github.com/henrywhitaker3/crds/internal/processor"
	"github.com/spf13/pflag"
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

	if err := processor.Process(ctx, conf.CRDs, conf.Collections); err != nil {
		slog.Error("failed processing", "error", err)
		os.Exit(1)
	}
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
