package graph

import (
	"encoding/gob"
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
	logFile       *os.File        // The append-only log file where operations are written
	logPath       string          // Path to the log file
	snapshotPath  string          // Path to the snapshot file
	mu            sync.Mutex      // Protects concurrent writes to the log
	buffer        []Operation     // Buffer for batching operations
	bufferSize    int             // Maximum buffer size before auto-flush
	syncMode      SyncMode        // When to sync to disk
	flushTimer    *time.Timer     // Timer for periodic flushes
	flushInterval time.Duration   // Interval for timer (stored for hybrid mode resets)
	flushChan     chan struct{}   // Channel to signal manual flush
	closeChan     chan struct{}   // Channel to signal WAL shutdown
}

// SyncMode controls when WAL writes are synced to disk
type SyncMode int

const (
	// SyncEveryWrite syncs after every write (safest, slowest)
	SyncEveryWrite SyncMode = iota
	// SyncBatch syncs after each batch (balanced)
	SyncBatch
	// SyncPeriodic syncs periodically (fastest, less durable)
	SyncPeriodic
	// SyncHybrid syncs on batch full OR timeout (best balance)
	SyncHybrid
)

// NewWAL creates a new Write-Ahead Log instance with default settings
// dataDir: directory where log and snapshot files will be stored
func NewWAL(dataDir string) (*WAL, error) {
	return NewWALWithOptions(dataDir, WALOptions{
		BufferSize:    100,                    // Batch up to 100 operations
		SyncMode:      SyncHybrid,             // Sync on batch full OR timeout
		FlushInterval: 100 * time.Millisecond, // Flush every 100ms if buffer not full
	})
}

// WALOptions configures WAL behavior
type WALOptions struct {
	BufferSize    int           // Maximum operations to buffer before auto-flush
	SyncMode      SyncMode      // When to sync to disk
	FlushInterval time.Duration // How often to auto-flush (for SyncPeriodic and SyncHybrid)
}

// NewWALWithOptions creates a WAL with custom options
func NewWALWithOptions(dataDir string, opts WALOptions) (*WAL, error) {
	// Ensure the data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	logPath := filepath.Join(dataDir, "wal.log")
	snapshotPath := filepath.Join(dataDir, "snapshot.gob")

	// Open log file in append mode - we never overwrite, only add new operations
	// CREATE flag creates the file if it doesn't exist
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		logFile:       logFile,
		logPath:       logPath,
		snapshotPath:  snapshotPath,
		buffer:        make([]Operation, 0, opts.BufferSize),
		bufferSize:    opts.BufferSize,
		syncMode:      opts.SyncMode,
		flushInterval: opts.FlushInterval,
		flushChan:     make(chan struct{}, 1),
		closeChan:     make(chan struct{}),
	}

	// Start background flusher for periodic and hybrid modes
	if (opts.SyncMode == SyncPeriodic || opts.SyncMode == SyncHybrid) && opts.FlushInterval > 0 {
		wal.flushTimer = time.NewTimer(opts.FlushInterval)
		go wal.backgroundFlusher(opts.FlushInterval)
	}

	return wal, nil
}

// WriteOperation appends an operation to the log
// This is called before every graph mutation to ensure durability
func (w *WAL) WriteOperation(op Operation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set the timestamp for this operation
	op.Timestamp = time.Now()

	// Add to buffer
	w.buffer = append(w.buffer, op)

	// Check if we should flush based on sync mode
	switch w.syncMode {
	case SyncEveryWrite:
		// Flush immediately for maximum durability
		return w.flushUnlocked()
	case SyncBatch:
		// Flush when buffer is full
		if len(w.buffer) >= w.bufferSize {
			return w.flushUnlocked()
		}
		return nil
	case SyncPeriodic:
		// Let background flusher handle it
		return nil
	case SyncHybrid:
		// Reset the timer on each write (inactivity timer)
		if w.flushTimer != nil {
			w.flushTimer.Reset(w.flushInterval)
		}
		// Flush when buffer is full
		if len(w.buffer) >= w.bufferSize {
			return w.flushUnlocked()
		}
		return nil
	default:
		return w.flushUnlocked()
	}
}

// Flush forces all buffered operations to be written to disk
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushUnlocked()
}

// flushUnlocked writes all buffered operations to disk (caller must hold lock)
func (w *WAL) flushUnlocked() error {
	if len(w.buffer) == 0 {
		return nil
	}

	// Write all buffered operations
	for _, op := range w.buffer {
		data, err := json.Marshal(op)
		if err != nil {
			return fmt.Errorf("failed to marshal operation: %w", err)
		}

		if _, err := w.logFile.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("failed to write operation to WAL: %w", err)
		}
	}

	// Sync to disk once for the entire batch
	if err := w.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	// Clear the buffer
	w.buffer = w.buffer[:0]
	return nil
}

// backgroundFlusher periodically flushes the buffer
func (w *WAL) backgroundFlusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.Flush()
		case <-w.flushChan:
			w.Flush()
		case <-w.closeChan:
			w.Flush() // Final flush before closing
			return
		}
	}
}

// CreateSnapshot saves the entire graph state to disk
// This is done periodically to avoid having to replay the entire log on startup
// After creating a snapshot, we can truncate the old log entries
func (w *WAL) CreateSnapshot(snapshot *Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set the snapshot timestamp
	snapshot.Timestamp = time.Now()

	// Write to a temporary file first, then atomically rename
	// This ensures we never have a partial/corrupted snapshot
	tempPath := w.snapshotPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// Serialize the entire graph state using Gob (binary format)
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	// Sync to disk before renaming
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot: %w", err)
	}

	file.Close()

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

	// Open the snapshot file
	file, err := os.Open(w.snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	// Deserialize the snapshot using Gob
	var snapshot Snapshot
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
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
	// Signal background flusher to stop
	if w.closeChan != nil {
		close(w.closeChan)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any remaining buffered operations
	if err := w.flushUnlocked(); err != nil {
		return err
	}

	if err := w.logFile.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	return nil
}
