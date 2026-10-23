package connector

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCurrentTokenPrecedence(t *testing.T) {
	dir := t.TempDir()
	credFile := filepath.Join(dir, "credential")

	// 无凭据文件、无 enroll token → legacy 共享 token。
	c := New("wss://hub/gateway/ws", "tg-a", "legacy-token")
	if got := c.currentToken(); got != "legacy-token" {
		t.Fatalf("legacy fallback = %q, want legacy-token", got)
	}

	// enroll token > legacy（凭据文件尚不存在时首拨用 enroll token）。
	c = New("wss://hub/gateway/ws", "tg-a", "legacy-token").
		WithEnrollBootstrap("enroll-token", credFile)
	if got := c.currentToken(); got != "enroll-token" {
		t.Fatalf("enroll bootstrap = %q, want enroll-token", got)
	}

	// 凭据文件 > enroll token > legacy（首拨成功后的稳态）。
	if err := os.WriteFile(credFile, []byte("per-target-cred\n"), 0o600); err != nil {
		t.Fatalf("seed credential file: %v", err)
	}
	if got := c.currentToken(); got != "per-target-cred" {
		t.Fatalf("persisted credential = %q, want per-target-cred", got)
	}
}

func TestPersistCredentialCreatesDirAndFile(t *testing.T) {
	dir := t.TempDir()
	credFile := filepath.Join(dir, "nested", "credential")

	c := New("wss://hub/gateway/ws", "tg-a", "legacy-token").
		WithEnrollBootstrap("enroll-token", credFile)
	c.persistCredential("per-target-cred")

	info, err := os.Stat(credFile)
	if err != nil {
		t.Fatalf("credential file not written: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("credential file mode = %v, want 0600", info.Mode().Perm())
	}
	b, err := os.ReadFile(credFile)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(b) != "per-target-cred\n" {
		t.Fatalf("content = %q, want newline-terminated credential", b)
	}
}

func TestPersistCredentialNoopWithoutFile(t *testing.T) {
	// 未配置 credentialFile（未接 WithEnrollBootstrap）：吞掉凭据不落盘，
	// legacy 路径继续用共享 token。
	c := New("wss://hub/gateway/ws", "tg-a", "legacy-token")
	c.persistCredential("per-target-cred") // must not panic / write anywhere
	if got := c.currentToken(); got != "legacy-token" {
		t.Fatalf("after noop persist = %q, want legacy-token", got)
	}
}
