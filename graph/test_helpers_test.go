package graph

import (
	"os"
	"testing"
)

// newTestGraph creates a DiskGraph in a temp directory for testing
// Returns the graph and a cleanup function that should be deferred
func newTestGraph(t *testing.T) (*DiskGraph, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "gravecdb-test-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	db, err := NewDiskGraph(tmpDir, 1000)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create DiskGraph: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}
