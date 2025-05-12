package graft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/tidwall/wal"
	"go.etcd.io/bbolt"
)

type SnapshotService interface {
	writeSnapshotToDisk(snapshot *Snapshot)
}

type BoltSnapshotService struct {
	metadataDB *bbolt.DB // metadata storage
	walDB      *wal.Log
}

func (s BoltSnapshotService) writeSnapshotToDisk(snapshot *Snapshot) {
	s.metadataDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("RaftSnapshotStore"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	batch := new(wal.Batch)
	log.Println("Writing snapshot to disk:", snapshot.logSnapshot)
	for _, log := range snapshot.logSnapshot {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(log); err != nil {
			panic(fmt.Sprintf("Unable to encode log index %d", log.Index))
		}
		// batch.Write(uint64(log.Index+1), []byte(buf.Bytes()))
	}
	err := s.walDB.WriteBatch(batch)
	if err != nil {
		panic("Unable to write snapshot to WAL " + err.Error())
	}
}

type Snapshot struct {
	// state needed to snapshot to database
	lastIncludedIndex int32
	lastIncludedTerm  int32
	logSnapshot       []LogEntry
}

func newBoltSnapshotService(peerSnapshotFile string, peerSnapshotMetadataFile string) BoltSnapshotService {
	metadataDB, err := bbolt.Open(peerSnapshotMetadataFile, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic("Unable to create boltDB connection for peer metadata file: " + err.Error())
	}
	log, err := wal.Open(peerSnapshotFile, nil)
	if err != nil {
		panic("Unable to create WAL connection for peer file: " + err.Error())
	}
	fmt.Println("Creating bolt snapshot service for " + peerSnapshotFile)
	return BoltSnapshotService{metadataDB, log}
}
