package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func newTestDB(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "db"), opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return db, dir
}

func TestSmokeBasicReadWrite(t *testing.T) {
	db, _ := newTestDB(t, DefaultOptions())
	defer db.Close()

	if err := db.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := db.Get("k1")
	if err != nil || string(got) != "v1" {
		t.Fatalf("Get = %q, %v; want v1", got, err)
	}

	// 快照隔离：读事务打开后，其他提交对它不可见。
	rt, err := db.BeginReadOnly()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("k1", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	got, err = rt.Get("k1")
	if err != nil || string(got) != "v1" {
		t.Fatalf("snapshot Get = %q, %v; want v1 (older view)", got, err)
	}
	rt.Rollback()

	got, _ = db.Get("k1")
	if string(got) != "v2" {
		t.Fatalf("latest Get = %q; want v2", got)
	}

	// 删除语义。
	if err := db.Delete("k1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get("k1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after Delete Get err = %v; want ErrNotFound", err)
	}
}

func TestSmokeRestartDurability(t *testing.T) {
	opts := DefaultOptions()
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}} {
		got, err := db2.Get(kv[0])
		if err != nil || string(got) != kv[1] {
			t.Fatalf("after restart %s = %q, %v; want %s", kv[0], got, err, kv[1])
		}
	}
}
