package memstore

import (
	"testing"

	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return New() })
}
