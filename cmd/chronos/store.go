package main

import (
	"fmt"
	"log/slog"

	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
	"github.com/krishhna24/chronos/internal/store/pebblestore"
)

func openStore(cfg config, log *slog.Logger) (store.Store, error) {
	switch cfg.storage {
	case "memory":
		log.Warn("using in-memory storage: history is lost on restart")
		return memstore.New(), nil
	case "pebble":
		s, err := pebblestore.Open(cfg.dataDir, pebblestore.Options{Logger: log})
		if err != nil {
			return nil, fmt.Errorf("open store in %s: %w", cfg.dataDir, err)
		}
		log.Info("opened pebble store", "dir", cfg.dataDir)
		return s, nil
	}
	return nil, fmt.Errorf("--storage must be pebble or memory, got %q", cfg.storage)
}
