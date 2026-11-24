package main

import (
	"fmt"
	"log/slog"

	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

func openStore(cfg config, log *slog.Logger) (store.Store, error) {
	switch cfg.storage {
	case "memory":
		log.Warn("using in-memory storage: history is lost on restart")
		return memstore.New(), nil
	case "pebble":
		return nil, fmt.Errorf("pebble storage not available in this build yet")
	}
	return nil, fmt.Errorf("--storage must be pebble or memory, got %q", cfg.storage)
}
