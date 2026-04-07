package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// EntryType defines the type of operation in the log
type EntryType byte

const (
	TypePut    EntryType = 1
	TypeDelete EntryType = 2
)

// Entry represents a single record in the log
type Entry struct {
	Type  EntryType
	Key   string
	Value []byte
}

// Engine handles the persistence and retrieval of key-value pairs
type Engine struct {
	mu     sync.RWMutex
	index  map[string][]byte
	logDir string
	wal    *os.File
}

// NewEngine creates a new storage engine
func NewEngine(logDir string) (*Engine, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(logDir, "wal.log")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		index:  make(map[string][]byte),
		logDir: logDir,
		wal:    file,
	}

	if err := e.load(); err != nil {
		return nil, fmt.Errorf("failed to load log: %w", err)
	}

	return e, nil
}

// Put adds or updates a key-value pair
func (e *Engine) Put(key string, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := Entry{Type: TypePut, Key: key, Value: value}
	if err := e.appendLog(entry); err != nil {
		return err
	}

	e.index[key] = value
	return nil
}

// Get retrieves a value by key
func (e *Engine) Get(key string) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, ok := e.index[key]
	return val, ok
}

// Delete removes a key
func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := Entry{Type: TypeDelete, Key: key}
	if err := e.appendLog(entry); err != nil {
		return err
	}

	delete(e.index, key)
	return nil
}

// appendLog writes an entry to the WAL
func (e *Engine) appendLog(entry Entry) error {
	data := e.encodeEntry(entry)
	if _, err := e.wal.Write(data); err != nil {
		return err
	}
	return e.wal.Sync() // Ensure durability
}

// encodeEntry serializes an entry to binary
// Format: [Type (1b)][KeyLen (4b)][Key][ValLen (4b)][Value]
func (e *Engine) encodeEntry(entry Entry) []byte {
	kLen := uint32(len(entry.Key))
	vLen := uint32(len(entry.Value))
	buf := make([]byte, 1+4+kLen+4+vLen)

	buf[0] = byte(entry.Type)
	binary.BigEndian.PutUint32(buf[1:5], kLen)
	copy(buf[5:5+kLen], entry.Key)
	binary.BigEndian.PutUint32(buf[5+kLen:9+kLen], vLen)
	copy(buf[9+kLen:], entry.Value)

	return buf
}

// load rebuilds the index from the WAL
func (e *Engine) load() error {
	if _, err := e.wal.Seek(0, 0); err != nil {
		return err
	}

	for {
		header := make([]byte, 1)
		if _, err := io.ReadFull(e.wal, header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		entryType := EntryType(header[0])

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(e.wal, lenBuf); err != nil {
			return err
		}
		kLen := binary.BigEndian.Uint32(lenBuf)
		key := make([]byte, kLen)
		if _, err := io.ReadFull(e.wal, key); err != nil {
			return err
		}

		if _, err := io.ReadFull(e.wal, lenBuf); err != nil {
			return err
		}
		vLen := binary.BigEndian.Uint32(lenBuf)
		value := make([]byte, vLen)
		if _, err := io.ReadFull(e.wal, value); err != nil {
			return err
		}

		if entryType == TypePut {
			e.index[string(key)] = value
		} else if entryType == TypeDelete {
			delete(e.index, string(key))
		}
	}

	// Move file pointer back to end for future appends
	_, err := e.wal.Seek(0, 2)
	return err
}

func (e *Engine) Close() error {
	return e.wal.Close()
}
