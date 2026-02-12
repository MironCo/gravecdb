package graph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Operation represents a single database operation that can be logged
// This allows us to replay operations to reconstruct the database state
type Operation struct {
	Type      string                 // Type of operation: "CREATE_NODE", "CREATE_REL", "SET_PROP", "DELETE_NODE", "DELETE_REL"
	Timestamp time.Time              // When the operation occurred
	Data      map[string]interface{} // Operation-specific data
}

// Snapshot represents a complete state of the graph at a point in time
// Used for faster recovery - instead of replaying all operations from the beginning,
// we can load the latest snapshot and only replay operations after that
type Snapshot struct {
	Timestamp     time.Time                   // When this snapshot was created
	Nodes         map[string]*Node            // All nodes in the graph
	Relationships map[string]*Relationship    // All relationships in the graph
	NodesByLabel  map[string]map[string]*Node // Index of nodes by their labels
}

// WAL (Write-Ahead Log) manages persistence of graph operations to disk
// Every operation is first written to the log before being applied to the in-memory graph
// This ensures durability - if the process crashes, we can replay the log to recover state
type WAL struct {
	logFile      *os.File    // The append-only log file where operations are written
	logPath      string      // Path to the log file
	snapshotPath string      // Path to the snapshot file
	mu           sync.Mutex  // Protects concurrent writes to the log
}

// NewWAL creates a new Write-Ahead Log instance
// dataDir: directory where log and snapshot files will be stored
func NewWAL(dataDir string) (*WAL, error) {
	// Ensure the data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	logPath := filepath.Join(dataDir, "wal.log")
	snapshotPath := filepath.Join(dataDir, "snapshot.json")

	// Open log file in append mode - we never overwrite, only add new operations
	// CREATE flag creates the file if it doesn't exist
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		logFile:      logFile,
		logPath:      logPath,
		snapshotPath: snapshotPath,
	}, nil
}

// WriteOperation appends an operation to the log
// This is called before every graph mutation to ensure durability
func (w *WAL) WriteOperation(op Operation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set the timestamp for this operation
	op.Timestamp = time.Now()

	// Serialize the operation to JSON
	// Each operation is written as a single line (newline-delimited JSON)
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}

	// Append the operation to the log file with a newline separator
	if _, err := w.logFile.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write operation to WAL: %w", err)
	}

	// Force the OS to flush the data to disk immediately
	// This ensures durability - the operation is not considered committed until it's on disk
	if err := w.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// CreateSnapshot saves the entire graph state to disk
// This is done periodically to avoid having to replay the entire log on startup
// After creating a snapshot, we can truncate the old log entries
func (w *WAL) CreateSnapshot(snapshot *Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set the snapshot timestamp
	snapshot.Timestamp = time.Now()

	// Serialize the entire graph state to JSON
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Write to a temporary file first, then atomically rename
	// This ensures we never have a partial/corrupted snapshot
	tempPath := w.snapshotPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	// Atomic rename - if this succeeds, the snapshot is valid
	if err := os.Rename(tempPath, w.snapshotPath); err != nil {
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}

	return nil
}

// LoadSnapshot reads the latest snapshot from disk
// Returns nil if no snapshot exists (first startup)
func (w *WAL) LoadSnapshot() (*Snapshot, error) {
	// Check if snapshot file exists
	if _, err := os.Stat(w.snapshotPath); os.IsNotExist(err) {
		return nil, nil // No snapshot yet, return nil (not an error)
	}

	// Read the snapshot file
	data, err := os.ReadFile(w.snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	// Deserialize the snapshot
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}

// ReadOperations reads all operations from the log file
// Used during recovery to replay operations after loading a snapshot
func (w *WAL) ReadOperations() ([]Operation, error) {
	// Check if log file exists
	if _, err := os.Stat(w.logPath); os.IsNotExist(err) {
		return []Operation{}, nil // No log yet, return empty list
	}

	// Open the log file for reading
	file, err := os.Open(w.logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	defer file.Close()

	// Parse newline-delimited JSON
	var operations []Operation
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var op Operation
		if err := decoder.Decode(&op); err != nil {
			return nil, fmt.Errorf("failed to decode operation: %w", err)
		}
		operations = append(operations, op)
	}

	return operations, nil
}

// TruncateLog clears the log file
// Called after creating a snapshot - we don't need old operations anymore
func (w *WAL) TruncateLog() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close the current log file
	if err := w.logFile.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Truncate the file to zero bytes
	if err := os.Truncate(w.logPath, 0); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	// Reopen the log file for appending
	logFile, err := os.OpenFile(w.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL: %w", err)
	}

	w.logFile = logFile
	return nil
}

// Close closes the WAL and flushes any pending writes
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	if err := w.logFile.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	return nil
}
