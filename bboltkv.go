package bboltkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"go.etcd.io/bbolt"
	"time"
)

// Store represents the key value store. Use the Open() method to create
// one, and Close() it when done.
type Store struct {
	db         *bbolt.DB
	bucketName []byte
}

var (
	// ErrNotFound is returned when the key supplied to a Get or Delete
	// method does not exist in the database.
	ErrNotFound = errors.New("bboltkv: key not found")

	// ErrBadValue is returned when the value supplied to the Put method
	// is nil.
	ErrBadValue = errors.New("bboltkv: bad value")
)

// Open a key-value store. "path" is the full path to the database file, any
// leading directories must have been created already. File is created with
// mode 0640 if needed.
//
// Because of bboltDB restrictions, only one process may open the file at a
// time. Attempts to open the file from another process will fail with a
// timeout error.
func Open(path string, bucketName string) (*Store, error) {
	opts := &bbolt.Options{
		Timeout: 50 * time.Millisecond,
	}
	if db, err := bbolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &Store{db: db, bucketName: []byte(bucketName)}, nil
		}
	}
}

// Put an entry into the store. The passed value is gob-encoded and stored.
// The key can be an empty string, but the value cannot be nil - if it is,
// Put() returns ErrBadValue.
//
//	err := store.Put("key", 1)
//	err := store.Put("key", "string")
//	m := map[string]int{
//	    "harry": 1,
//	    "emma":  2,
//	}
//	err := store.Put("key", m)
func (s *Store) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(s.bucketName).Put([]byte(key), buf.Bytes())
	})
}

// Get an entry from the store. "value" must be a pointer-typed. If the key
// is not present in the store, Get returns ErrNotFound.
//
//	type MyStruct struct {
//	    Numbers []int
//	}
//	var val MyStruct
//	if err := store.Get("key", &val); err == skv.ErrNotFound {
//	    // "key" not found
//	} else if err != nil {
//	    // an error occurred
//	} else {
//	    // ok
//	}
//
// The value passed to Get() can be nil, in which case any value read from
// the store is silently discarded.
//
//  if err := store.Get("key", nil); err == nil {
//      fmt.Println("entry is present")
//  }
func (s *Store) Get(key string, value interface{}) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(s.bucketName).Cursor()
		if k, v := c.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else if value == nil {
			return nil
		} else {
			d := gob.NewDecoder(bytes.NewReader(v))
			return d.Decode(value)
		}
	})
}

// Delete the entry with the given key. If no such key is present in the store,
// it returns ErrNotFound.
//
//	store.Delete("key")
func (s *Store) Delete(key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(s.bucketName).Cursor()
		if k, _ := c.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else {
			return c.Delete()
		}
	})
}

// Close closes the key-value store file.
func (s *Store) Close() error {
	return s.db.Close()
}

// GetDb Get the database object directly to work with it
func (s *Store) GetDb() *bbolt.DB {
	return s.db
}

// GetBucketName Get the bucket name
func (s *Store) GetBucketName() []byte {
	return s.bucketName
}
