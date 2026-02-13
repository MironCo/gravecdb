package graph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// BoltStore provides bbolt-based persistence for the graph database
// Replaces WAL + Snapshots with a single transactional key-value store
type BoltStore struct {
	db *bolt.DB
}

// Bucket names
var (
	nodesBucket         = []byte("nodes")
	relationshipsBucket = []byte("relationships")
	embeddingsBucket    = []byte("embeddings")
	labelIndexBucket    = []byte("label_index") // label:nodeID -> nodeID
	metaBucket          = []byte("meta")
)

// NewBoltStore creates a new bbolt-backed storage engine
func NewBoltStore(dataDir string) (*BoltStore, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, "gravecdb.db")

	// Open bbolt database
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{
			nodesBucket,
			relationshipsBucket,
			embeddingsBucket,
			labelIndexBucket,
			metaBucket,
		} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltStore{db: db}, nil
}

// SaveNode persists a node to bbolt
func (s *BoltStore) SaveNode(node *Node) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)

		// Serialize node to JSON
		data, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal node: %w", err)
		}

		// Store node by ID
		if err := b.Put([]byte(node.ID), data); err != nil {
			return fmt.Errorf("failed to save node: %w", err)
		}

		// Update label index
		lb := tx.Bucket(labelIndexBucket)
		for _, label := range node.Labels {
			indexKey := []byte(label + ":" + node.ID)
			if err := lb.Put(indexKey, []byte(node.ID)); err != nil {
				return fmt.Errorf("failed to update label index: %w", err)
			}
		}

		return nil
	})
}

// GetNode retrieves a node by ID
func (s *BoltStore) GetNode(id string) (*Node, error) {
	var node *Node

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)
		data := b.Get([]byte(id))

		if data == nil {
			return nil // Node not found
		}

		node = &Node{}
		if err := json.Unmarshal(data, node); err != nil {
			return fmt.Errorf("failed to unmarshal node: %w", err)
		}

		return nil
	})

	return node, err
}

// GetAllNodes retrieves all nodes from the database
func (s *BoltStore) GetAllNodes() ([]*Node, error) {
	var nodes []*Node

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var node Node
			if err := json.Unmarshal(v, &node); err != nil {
				return fmt.Errorf("failed to unmarshal node: %w", err)
			}
			nodes = append(nodes, &node)
		}

		return nil
	})

	return nodes, err
}

// GetNodesByLabel retrieves all nodes with a specific label
func (s *BoltStore) GetNodesByLabel(label string) ([]*Node, error) {
	var nodes []*Node

	err := s.db.View(func(tx *bolt.Tx) error {
		lb := tx.Bucket(labelIndexBucket)
		nb := tx.Bucket(nodesBucket)
		c := lb.Cursor()

		// Seek to the first key with this label prefix
		prefix := []byte(label + ":")
		for k, _ := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, _ = c.Next() {
			// Extract node ID from index key (format: "label:nodeID")
			nodeID := k[len(prefix):]

			// Fetch the actual node
			data := nb.Get(nodeID)
			if data != nil {
				var node Node
				if err := json.Unmarshal(data, &node); err != nil {
					return fmt.Errorf("failed to unmarshal node: %w", err)
				}
				nodes = append(nodes, &node)
			}
		}

		return nil
	})

	return nodes, err
}

// DeleteNode marks a node as deleted (soft delete - sets ValidTo)
func (s *BoltStore) DeleteNode(id string, deletedAt time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)

		// Get existing node
		data := b.Get([]byte(id))
		if data == nil {
			return fmt.Errorf("node not found: %s", id)
		}

		var node Node
		if err := json.Unmarshal(data, &node); err != nil {
			return fmt.Errorf("failed to unmarshal node: %w", err)
		}

		// Set ValidTo (soft delete)
		node.ValidTo = &deletedAt

		// Save updated node
		updatedData, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal node: %w", err)
		}

		return b.Put([]byte(id), updatedData)
	})
}

// SaveRelationship persists a relationship to bbolt
func (s *BoltStore) SaveRelationship(rel *Relationship) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)

		data, err := json.Marshal(rel)
		if err != nil {
			return fmt.Errorf("failed to marshal relationship: %w", err)
		}

		return b.Put([]byte(rel.ID), data)
	})
}

// GetRelationship retrieves a relationship by ID
func (s *BoltStore) GetRelationship(id string) (*Relationship, error) {
	var rel *Relationship

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)
		data := b.Get([]byte(id))

		if data == nil {
			return nil
		}

		rel = &Relationship{}
		if err := json.Unmarshal(data, rel); err != nil {
			return fmt.Errorf("failed to unmarshal relationship: %w", err)
		}

		return nil
	})

	return rel, err
}

// GetAllRelationships retrieves all relationships from the database
func (s *BoltStore) GetAllRelationships() ([]*Relationship, error) {
	var rels []*Relationship

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var rel Relationship
			if err := json.Unmarshal(v, &rel); err != nil {
				return fmt.Errorf("failed to unmarshal relationship: %w", err)
			}
			rels = append(rels, &rel)
		}

		return nil
	})

	return rels, err
}

// DeleteRelationship marks a relationship as deleted (soft delete)
func (s *BoltStore) DeleteRelationship(id string, deletedAt time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)

		data := b.Get([]byte(id))
		if data == nil {
			return fmt.Errorf("relationship not found: %s", id)
		}

		var rel Relationship
		if err := json.Unmarshal(data, &rel); err != nil {
			return fmt.Errorf("failed to unmarshal relationship: %w", err)
		}

		rel.ValidTo = &deletedAt

		updatedData, err := json.Marshal(rel)
		if err != nil {
			return fmt.Errorf("failed to marshal relationship: %w", err)
		}

		return b.Put([]byte(id), updatedData)
	})
}

// SaveEmbedding persists a node embedding (all versions)
func (s *BoltStore) SaveEmbedding(nodeID string, embeddings []*Embedding) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(embeddingsBucket)

		data, err := json.Marshal(embeddings)
		if err != nil {
			return fmt.Errorf("failed to marshal embeddings: %w", err)
		}

		return b.Put([]byte(nodeID), data)
	})
}

// GetEmbedding retrieves all embedding versions for a node
func (s *BoltStore) GetEmbedding(nodeID string) ([]*Embedding, error) {
	var embeddings []*Embedding

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(embeddingsBucket)
		data := b.Get([]byte(nodeID))

		if data == nil {
			return nil
		}

		if err := json.Unmarshal(data, &embeddings); err != nil {
			return fmt.Errorf("failed to unmarshal embeddings: %w", err)
		}

		return nil
	})

	return embeddings, err
}

// GetAllEmbeddings retrieves all embeddings
func (s *BoltStore) GetAllEmbeddings() (map[string][]*Embedding, error) {
	embeddings := make(map[string][]*Embedding)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(embeddingsBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var embs []*Embedding
			if err := json.Unmarshal(v, &embs); err != nil {
				return fmt.Errorf("failed to unmarshal embeddings: %w", err)
			}
			embeddings[string(k)] = embs
		}

		return nil
	})

	return embeddings, err
}

// Close closes the bbolt database
func (s *BoltStore) Close() error {
	return s.db.Close()
}

// Stats returns database statistics
func (s *BoltStore) Stats() (*bolt.Stats, error) {
	stats := s.db.Stats()
	return &stats, nil
}
