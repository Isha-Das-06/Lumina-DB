package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestEngine(t *testing.T) {
	logDir := "test_logs"
	defer os.RemoveAll(logDir)

	engine, err := NewEngine(logDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// Test Put and Get
	key := "hello"
	val := []byte("world")
	if err := engine.Put(key, val); err != nil {
		t.Errorf("Put failed: %v", err)
	}

	got, ok := engine.Get(key)
	if !ok || !bytes.Equal(got, val) {
		t.Errorf("Get failed: got %v, want %v", got, val)
	}

	// Test Persistence
	engine.Close()
	engine, err = NewEngine(logDir)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer engine.Close()

	got, ok = engine.Get(key)
	if !ok || !bytes.Equal(got, val) {
		t.Errorf("Persistence failed: got %v, want %v", got, val)
	}

	// Test Delete
	if err := engine.Delete(key); err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	_, ok = engine.Get(key)
	if ok {
		t.Errorf("Delete failed: key still exists")
	}
}
