package rpcproxy

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Engines []Engine `yaml:"engines"`
}

type Engine struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

func LoadConfig(r io.Reader) (Config, error) {
	var cfg Config
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode manifest: %w", err)
	}
	if len(cfg.Engines) == 0 {
		return Config{}, fmt.Errorf("manifest must contain at least one engine")
	}
	for i, engine := range cfg.Engines {
		if strings.TrimSpace(engine.Name) == "" {
			return Config{}, fmt.Errorf("engine %d: name must not be empty", i)
		}
		if strings.TrimSpace(engine.Address) == "" {
			return Config{}, fmt.Errorf("engine %q: address must not be empty", engine.Name)
		}
	}
	return cfg, nil
}
