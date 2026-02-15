package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/MironCo/gravecdb/core"
	"github.com/MironCo/gravecdb/embedding"
	bolt "go.etcd.io/bbolt"
)

// BoltStore provides bbolt-based persistence for the graph database
type BoltStore struct {
	db *bolt.DB
}

// Bucket names
var (
	nodesBucket         = []byte("nodes")
	relationshipsBucket = []byte("relationships")
	embeddingsBucket    = []byte("embeddings")
	labelIndexBucket    = []byte("label_index")
	metaBucket          = []byte("meta")
)

// NewBoltStore creates a new bbolt-backed storage engine
func NewBoltStore(dataDir string) (*BoltStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, "gravecdb.db")

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

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
func (s *BoltStore) SaveNode(node *core.Node) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)

		data, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal node: %w", err)
		}

		key := fmt.Sprintf("%s:%d", node.ID, node.ValidFrom.UnixNano())
		if err := b.Put([]byte(key), data); err != nil {
			return fmt.Errorf("failed to save node: %w", err)
		}

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

// GetNode retrieves the current version of a node by ID
func (s *BoltStore) GetNode(id string) (*core.Node, error) {
	var node *core.Node

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)
		c := b.Cursor()

		prefix := []byte(id + ":")
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var n core.Node
			if err := json.Unmarshal(v, &n); err != nil {
				return fmt.Errorf("failed to unmarshal node: %w", err)
			}

			if n.ValidTo == nil {
				node = &n
				return nil
			}
		}

		return nil
	})

	return node, err
}

// GetAllNodes retrieves all nodes from the database
func (s *BoltStore) GetAllNodes() ([]*core.Node, error) {
	var nodes []*core.Node

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var node core.Node
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
func (s *BoltStore) GetNodesByLabel(label string) ([]*core.Node, error) {
	var nodes []*core.Node

	err := s.db.View(func(tx *bolt.Tx) error {
		lb := tx.Bucket(labelIndexBucket)
		nb := tx.Bucket(nodesBucket)
		c := lb.Cursor()

		prefix := []byte(label + ":")
		for k, _ := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, _ = c.Next() {
			nodeID := k[len(prefix):]

			data := nb.Get(nodeID)
			if data != nil {
				var node core.Node
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

// DeleteNode marks a node as deleted (soft delete)
func (s *BoltStore) DeleteNode(id string, deletedAt time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodesBucket)
		c := b.Cursor()

		prefix := []byte(id + ":")
		var foundKey []byte
		var node core.Node

		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var n core.Node
			if err := json.Unmarshal(v, &n); err != nil {
				return fmt.Errorf("failed to unmarshal node: %w", err)
			}

			if n.ValidTo == nil {
				foundKey = k
				node = n
				break
			}
		}

		if foundKey == nil {
			return fmt.Errorf("node not found: %s", id)
		}

		node.ValidTo = &deletedAt

		updatedData, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal node: %w", err)
		}

		return b.Put(foundKey, updatedData)
	})
}

// SaveRelationship persists a relationship to bbolt
func (s *BoltStore) SaveRelationship(rel *core.Relationship) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)

		data, err := json.Marshal(rel)
		if err != nil {
			return fmt.Errorf("failed to marshal relationship: %w", err)
		}

		key := fmt.Sprintf("%s:%d", rel.ID, rel.ValidFrom.UnixNano())
		return b.Put([]byte(key), data)
	})
}

// GetRelationship retrieves the current version of a relationship by ID
func (s *BoltStore) GetRelationship(id string) (*core.Relationship, error) {
	var rel *core.Relationship

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)
		c := b.Cursor()

		prefix := []byte(id + ":")
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var r core.Relationship
			if err := json.Unmarshal(v, &r); err != nil {
				return fmt.Errorf("failed to unmarshal relationship: %w", err)
			}

			if r.ValidTo == nil {
				rel = &r
				return nil
			}
		}

		return nil
	})

	return rel, err
}

// GetAllRelationships retrieves all relationships from the database
func (s *BoltStore) GetAllRelationships() ([]*core.Relationship, error) {
	var rels []*core.Relationship

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(relationshipsBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var rel core.Relationship
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
		c := b.Cursor()

		prefix := []byte(id + ":")
		var foundKey []byte
		var rel core.Relationship

		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var r core.Relationship
			if err := json.Unmarshal(v, &r); err != nil {
				return fmt.Errorf("failed to unmarshal relationship: %w", err)
			}

			if r.ValidTo == nil {
				foundKey = k
				rel = r
				break
			}
		}

		if foundKey == nil {
			return fmt.Errorf("relationship not found: %s", id)
		}

		rel.ValidTo = &deletedAt

		updatedData, err := json.Marshal(rel)
		if err != nil {
			return fmt.Errorf("failed to marshal relationship: %w", err)
		}

		return b.Put(foundKey, updatedData)
	})
}

// SaveEmbedding persists node embeddings
func (s *BoltStore) SaveEmbedding(nodeID string, embeddings []*embedding.Embedding) error {
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
func (s *BoltStore) GetEmbedding(nodeID string) ([]*embedding.Embedding, error) {
	var embeddings []*embedding.Embedding

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
func (s *BoltStore) GetAllEmbeddings() (map[string][]*embedding.Embedding, error) {
	embeddings := make(map[string][]*embedding.Embedding)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(embeddingsBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var embs []*embedding.Embedding
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
func (s *BoltStore) Stats() *bolt.Stats {
	stats := s.db.Stats()
	return &stats
}

// Tx wraps a bbolt transaction
type Tx struct {
	btx       *bolt.Tx
	writable  bool
	committed bool
}

// BeginTx starts a new transaction
func (s *BoltStore) BeginTx(writable bool) (*Tx, error) {
	btx, err := s.db.Begin(writable)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &Tx{btx: btx, writable: writable}, nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed or rolled back")
	}
	tx.committed = true
	return tx.btx.Commit()
}

// Rollback aborts the transaction
func (tx *Tx) Rollback() error {
	if tx.committed {
		return nil
	}
	tx.committed = true
	return tx.btx.Rollback()
}

// SaveNode persists a node within a transaction
func (tx *Tx) SaveNode(node *core.Node) error {
	if !tx.writable {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	b := tx.btx.Bucket(nodesBucket)
	if b == nil {
		return fmt.Errorf("nodes bucket not found")
	}

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := fmt.Sprintf("%s:%d", node.ID, node.ValidFrom.UnixNano())
	if err := b.Put([]byte(key), data); err != nil {
		return fmt.Errorf("failed to save node: %w", err)
	}

	lb := tx.btx.Bucket(labelIndexBucket)
	if lb != nil {
		for _, label := range node.Labels {
			indexKey := []byte(label + ":" + node.ID)
			if err := lb.Put(indexKey, []byte(node.ID)); err != nil {
				return fmt.Errorf("failed to update label index: %w", err)
			}
		}
	}

	return nil
}

// GetNode retrieves a node within a transaction
func (tx *Tx) GetNode(id string) (*core.Node, error) {
	b := tx.btx.Bucket(nodesBucket)
	if b == nil {
		return nil, fmt.Errorf("nodes bucket not found")
	}

	c := b.Cursor()
	prefix := []byte(id + ":")

	for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
		var n core.Node
		if err := json.Unmarshal(v, &n); err != nil {
			return nil, fmt.Errorf("failed to unmarshal node: %w", err)
		}

		if n.ValidTo == nil {
			return &n, nil
		}
	}

	return nil, nil
}

// DeleteNode marks a node as deleted within a transaction
func (tx *Tx) DeleteNode(id string, deletedAt time.Time) error {
	if !tx.writable {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	b := tx.btx.Bucket(nodesBucket)
	if b == nil {
		return fmt.Errorf("nodes bucket not found")
	}

	c := b.Cursor()
	prefix := []byte(id + ":")
	var foundKey []byte
	var node core.Node

	for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
		var n core.Node
		if err := json.Unmarshal(v, &n); err != nil {
			return fmt.Errorf("failed to unmarshal node: %w", err)
		}

		if n.ValidTo == nil {
			foundKey = k
			node = n
			break
		}
	}

	if foundKey == nil {
		return fmt.Errorf("node not found: %s", id)
	}

	node.ValidTo = &deletedAt

	updatedData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	return b.Put(foundKey, updatedData)
}

// SaveRelationship persists a relationship within a transaction
func (tx *Tx) SaveRelationship(rel *core.Relationship) error {
	if !tx.writable {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	b := tx.btx.Bucket(relationshipsBucket)
	if b == nil {
		return fmt.Errorf("relationships bucket not found")
	}

	data, err := json.Marshal(rel)
	if err != nil {
		return fmt.Errorf("failed to marshal relationship: %w", err)
	}

	key := fmt.Sprintf("%s:%d", rel.ID, rel.ValidFrom.UnixNano())
	return b.Put([]byte(key), data)
}

// GetRelationship retrieves a relationship within a transaction
func (tx *Tx) GetRelationship(id string) (*core.Relationship, error) {
	b := tx.btx.Bucket(relationshipsBucket)
	if b == nil {
		return nil, fmt.Errorf("relationships bucket not found")
	}

	c := b.Cursor()
	prefix := []byte(id + ":")

	for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
		var r core.Relationship
		if err := json.Unmarshal(v, &r); err != nil {
			return nil, fmt.Errorf("failed to unmarshal relationship: %w", err)
		}

		if r.ValidTo == nil {
			return &r, nil
		}
	}

	return nil, nil
}

// DeleteRelationship marks a relationship as deleted within a transaction
func (tx *Tx) DeleteRelationship(id string, deletedAt time.Time) error {
	if !tx.writable {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	b := tx.btx.Bucket(relationshipsBucket)
	if b == nil {
		return fmt.Errorf("relationships bucket not found")
	}

	c := b.Cursor()
	prefix := []byte(id + ":")
	var foundKey []byte
	var rel core.Relationship

	for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
		var r core.Relationship
		if err := json.Unmarshal(v, &r); err != nil {
			return fmt.Errorf("failed to unmarshal relationship: %w", err)
		}

		if r.ValidTo == nil {
			foundKey = k
			rel = r
			break
		}
	}

	if foundKey == nil {
		return fmt.Errorf("relationship not found: %s", id)
	}

	rel.ValidTo = &deletedAt

	updatedData, err := json.Marshal(rel)
	if err != nil {
		return fmt.Errorf("failed to marshal relationship: %w", err)
	}

	return b.Put(foundKey, updatedData)
}

// GetAllNodes retrieves all nodes within a transaction
func (tx *Tx) GetAllNodes() ([]*core.Node, error) {
	b := tx.btx.Bucket(nodesBucket)
	if b == nil {
		return nil, fmt.Errorf("nodes bucket not found")
	}

	var nodes []*core.Node
	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		var node core.Node
		if err := json.Unmarshal(v, &node); err != nil {
			return nil, fmt.Errorf("failed to unmarshal node: %w", err)
		}
		nodes = append(nodes, &node)
	}

	return nodes, nil
}

// GetAllRelationships retrieves all relationships within a transaction
func (tx *Tx) GetAllRelationships() ([]*core.Relationship, error) {
	b := tx.btx.Bucket(relationshipsBucket)
	if b == nil {
		return nil, fmt.Errorf("relationships bucket not found")
	}

	var rels []*core.Relationship
	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		var rel core.Relationship
		if err := json.Unmarshal(v, &rel); err != nil {
			return nil, fmt.Errorf("failed to unmarshal relationship: %w", err)
		}
		rels = append(rels, &rel)
	}

	return rels, nil
}

// SaveEmbedding persists embeddings within a transaction
func (tx *Tx) SaveEmbedding(nodeID string, embeddings []*embedding.Embedding) error {
	if !tx.writable {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	b := tx.btx.Bucket(embeddingsBucket)
	if b == nil {
		return fmt.Errorf("embeddings bucket not found")
	}

	data, err := json.Marshal(embeddings)
	if err != nil {
		return fmt.Errorf("failed to marshal embeddings: %w", err)
	}

	return b.Put([]byte(nodeID), data)
}
