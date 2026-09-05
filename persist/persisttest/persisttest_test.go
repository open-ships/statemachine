package persisttest_test

import (
	"context"
	"testing"

	"github.com/open-ships/statemachine/persist"
	"github.com/open-ships/statemachine/persist/persisttest"
)

func TestMemoryStoreContract(t *testing.T) {
	persisttest.Run(t, func(*testing.T) persisttest.Fixture[string, int, persist.MemoryUnit[string]] {
		store := persist.NewMemoryStore(map[string]int{"present": 0})
		return persisttest.Fixture[string, int, persist.MemoryUnit[string]]{
			Store: store, Key: "present", Missing: "absent", Initial: 0, Next: 1,
			Load: func(ctx context.Context, key string) (int, error) {
				snapshot, err := store.Load(ctx, key)
				return snapshot.State, err
			},
		}
	})
}
