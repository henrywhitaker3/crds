// Package config
package config

import (
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

type CRD struct {
	Group    string   `yaml:"group"`
	Subgroup *string  `yaml:"subgroup"`
	Ref      string   `yaml:"ref"`
	Template string   `yaml:"template"`
	Names    []string `yaml:"names"`
}

type Collection struct {
	Group    string  `yaml:"group"`
	Parent   *string `yaml:"parent"`
	Ref      string  `yaml:"ref"`
	Template string  `yaml:"template"`
}

type Config struct {
	CRDs        []CRD        `yaml:"crds"`
	Collections []Collection `yaml:"collections"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	out := Config{}
	if err := yaml.Unmarshal(file, &out); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &out, nil
}
