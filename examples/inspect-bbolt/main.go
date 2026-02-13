package main

import (
	"encoding/json"
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

func main() {
	// Open the database in read-only mode
	db, err := bolt.Open("../../data/gravecdb.db", 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== BoltDB Structure ===\n")

	err = db.View(func(tx *bolt.Tx) error {
		// List all buckets
		fmt.Println("Buckets:")
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			count := b.Stats().KeyN
			fmt.Printf("  - %s (%d keys)\n", name, count)
			return nil
		})

		fmt.Println("\n=== NODES BUCKET ===")
		nodesBucket := tx.Bucket([]byte("nodes"))
		if nodesBucket != nil {
			c := nodesBucket.Cursor()
			count := 0
			for k, v := c.First(); k != nil && count < 3; k, v = c.Next() {
				fmt.Printf("\nKey: %s\n", k)

				// Pretty-print the JSON
				var prettyJSON map[string]interface{}
				if err := json.Unmarshal(v, &prettyJSON); err == nil {
					formatted, _ := json.MarshalIndent(prettyJSON, "  ", "  ")
					fmt.Printf("Value:\n  %s\n", formatted)
				}
				count++
			}
		}

		fmt.Println("\n=== RELATIONSHIPS BUCKET ===")
		relsBucket := tx.Bucket([]byte("relationships"))
		if relsBucket != nil {
			c := relsBucket.Cursor()
			count := 0
			for k, v := c.First(); k != nil && count < 3; k, v = c.Next() {
				fmt.Printf("\nKey: %s\n", k)

				var prettyJSON map[string]interface{}
				if err := json.Unmarshal(v, &prettyJSON); err == nil {
					formatted, _ := json.MarshalIndent(prettyJSON, "  ", "  ")
					fmt.Printf("Value:\n  %s\n", formatted)
				}
				count++
			}
		}

		fmt.Println("\n=== LABEL INDEX BUCKET ===")
		labelBucket := tx.Bucket([]byte("label_index"))
		if labelBucket != nil {
			c := labelBucket.Cursor()
			count := 0
			fmt.Println("(Shows first 10 entries)")
			for k, v := c.First(); k != nil && count < 10; k, v = c.Next() {
				fmt.Printf("  %s -> %s\n", k, v)
				count++
			}
		}

		fmt.Println("\n=== EMBEDDINGS BUCKET ===")
		embBucket := tx.Bucket([]byte("embeddings"))
		if embBucket != nil {
			stats := embBucket.Stats()
			if stats.KeyN == 0 {
				fmt.Println("  (empty - no embeddings generated yet)")
			} else {
				c := embBucket.Cursor()
				k, v := c.First()
				if k != nil {
					fmt.Printf("\nNode ID: %s\n", k)
					var embeddings []interface{}
					if err := json.Unmarshal(v, &embeddings); err == nil {
						formatted, _ := json.MarshalIndent(embeddings, "  ", "  ")
						fmt.Printf("Embedding versions:\n  %s\n", formatted)
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== DATABASE STATS ===")
	stats := db.Stats()
	fmt.Printf("Page size: %d bytes\n", stats.TxStats.PageCount)
	fmt.Printf("Free pages: %d\n", stats.FreePageN)
	fmt.Printf("Pending pages: %d\n", stats.PendingPageN)
}
