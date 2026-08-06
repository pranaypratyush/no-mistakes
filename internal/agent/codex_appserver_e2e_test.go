//go:build e2e

package agent

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/kunchenguid/no-mistakes/internal/shellenv"
)

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// TestCodexAppServerRealProcessUnixHandshake is deliberately narrower than a
// live model turn: it launches a temporary real Codex App Server, authenticates
// the Unix peer, completes the native initialization protocol, and exercises
// thread/list plus thread/start RPCs. It does not start a model turn, spend
// model quota, run a no-mistakes pipeline, or touch the caller's Codex home.
// A real thread/resume needs a rollout, which App Server does not persist until
// a turn starts; resume is therefore covered by the deterministic protocol test
// instead of making this safety check spend model quota.
func TestCodexAppServerRealProcessUnixHandshake(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Codex App Server transport requires authenticated Unix peer credentials")
	}
	codexBin, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("codex binary is not installed")
	}

	// The package TestMain may place TMPDIR under a long isolation path, while
	// Unix-domain sockets have an approximately 100-byte path ceiling. Keep the
	// real server's private root bounded just like the deterministic fake.
	root, err := os.MkdirTemp("/tmp", "nm-codex-real-")
	if err != nil {
		t.Fatalf("create bounded app-server temp root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	codexHome := filepath.Join(root, "codex-home")
	if err := os.MkdirAll(codexHome, 0o755); err != nil {
		t.Fatalf("create temporary Codex home: %v", err)
	}
	socketPath := filepath.Join(root, "app-server.sock")

	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 5*time.Second)
	probeCmd := exec.CommandContext(probeCtx, codexBin, "app-server", "--help")
	probeCmd.Dir = root
	probeCmd.Env = envReplacing(os.Environ(), "CODEX_HOME", codexHome)
	shellenv.ConfigureShellCommand(probeCmd)
	probeOutput, probeErr := shellenv.CombinedOutputShellCommand(probeCmd)
	cancelProbe()
	if probeErr != nil || !codexAppServerHelpSupportsListen(string(probeOutput)) {
		t.Skipf("installed codex does not support app-server --listen: %v\n%s", probeErr, probeOutput)
	}

	processCtx, cancelProcess := context.WithCancel(context.Background())
	cmd := exec.CommandContext(processCtx, codexBin, "app-server", "--listen", "unix://"+socketPath)
	cmd.Dir = root
	cmd.Env = envReplacing(os.Environ(), "CODEX_HOME", codexHome)
	var output synchronizedBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	shellenv.ConfigureShellCommand(cmd)
	if err := shellenv.StartShellCommand(cmd); err != nil {
		cancelProcess()
		t.Fatalf("start real Codex App Server: %v", err)
	}
	waitDone := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(waitDone)
	}()
	t.Cleanup(func() {
		cancelProcess()
		shellenv.TerminateShellCommandGroup(cmd)
		select {
		case <-waitDone:
		case <-time.After(5 * time.Second):
			t.Errorf("real Codex App Server did not exit")
		}
	})

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		select {
		case <-waitDone:
			t.Fatalf("real Codex App Server exited before readiness: %v\n%s", waitErr, output.String())
		case <-deadline.C:
			t.Fatalf("real Codex App Server socket was not ready\n%s", output.String())
		case <-ticker.C:
		}
	}

	rpcCtx, cancelRPC := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRPC()
	conn, peerPID, err := dialCodexAppServer(rpcCtx, socketPath)
	if err != nil {
		t.Fatalf("dial real Codex App Server: %v\n%s", err, output.String())
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if peerPID != cmd.Process.Pid {
		t.Fatalf("authenticated peer pid = %d, want app-server pid %d", peerPID, cmd.Process.Pid)
	}

	client := &codexAppClient{conn: conn}
	if err := client.call(rpcCtx, "initialize", map[string]any{
		"clientInfo": map[string]any{"name": "no_mistakes_e2e", "title": "no-mistakes e2e", "version": "test"},
	}, nil); err != nil {
		t.Fatalf("initialize real Codex App Server: %v\n%s", err, output.String())
	}
	if err := client.notify(rpcCtx, "initialized", map[string]any{}); err != nil {
		t.Fatalf("notify initialized: %v", err)
	}
	var threads struct {
		Data []any `json:"data"`
	}
	if err := client.call(rpcCtx, "thread/list", map[string]any{"limit": 1, "useStateDbOnly": true}, &threads); err != nil {
		t.Fatalf("thread/list real Codex App Server: %v\n%s", err, output.String())
	}
	var started codexAppThreadResponse
	if err := client.call(rpcCtx, "thread/start", map[string]any{
		"cwd": root, "approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": "danger-full-access", "serviceName": "no_mistakes_e2e",
	}, &started); err != nil {
		t.Fatalf("thread/start real Codex App Server: %v\n%s", err, output.String())
	}
	if started.Thread.ID == "" {
		t.Fatal("thread/start real Codex App Server returned no thread id")
	}
}

func TestCodexAppServerHelpSupportsListen(t *testing.T) {
	tests := []struct {
		name string
		help string
		want bool
	}{
		{name: "supported flag", help: "Usage: codex app-server [OPTIONS]\n  --listen <LISTEN>", want: true},
		{name: "supported equals form", help: "--listen=unix:///tmp/codex.sock", want: true},
		{name: "different listen flag", help: "--listen-address <ADDRESS>", want: false},
		{name: "app server without listen", help: "Usage: codex app-server [OPTIONS]\n  --analytics-default-enabled", want: false},
		{name: "empty", help: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := codexAppServerHelpSupportsListen(tt.help); got != tt.want {
				t.Fatalf("codexAppServerHelpSupportsListen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func codexAppServerHelpSupportsListen(help string) bool {
	for _, field := range strings.Fields(help) {
		if field == "--listen" || strings.HasPrefix(field, "--listen=") {
			return true
		}
	}
	return false
}

func envReplacing(environ []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environ)+1)
	for _, entry := range environ {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}
