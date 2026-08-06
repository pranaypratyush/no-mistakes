package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	gitinternal "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type fakeAppServerRequest struct {
	ID     int64           `json:"id,omitempty"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type fakeAppServerMessage struct {
	ID     int64             `json:"id,omitempty"`
	Method string            `json:"method,omitempty"`
	Params json.RawMessage   `json:"params,omitempty"`
	Result json.RawMessage   `json:"result,omitempty"`
	Error  *codexAppRPCError `json:"error,omitempty"`
}

type fakeAppServer struct {
	endpoint string
	done     chan error
	server   *http.Server
	listener net.Listener
}

func startFakeAppServer(t *testing.T, serve func(*websocket.Conn) error) *fakeAppServer {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Codex App Server transport requires authenticated Unix peer credentials")
	}
	// Unix-domain socket paths are capped at roughly 100 bytes on Linux. The
	// package test temp root includes the full test name, so use a bounded
	// private directory directly below the OS temp root.
	socketDir, err := os.MkdirTemp("", "nm-codex-app-")
	if err != nil {
		t.Fatalf("create fake app-server temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "codex.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen fake app-server: %v", err)
	}
	fake := &fakeAppServer{
		endpoint: "unix://" + socketPath,
		done:     make(chan error, 1),
		listener: listener,
	}
	fake.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			fake.done <- fmt.Errorf("accept websocket: %w", err)
			return
		}
		defer conn.CloseNow()
		fake.done <- serve(conn)
	})}
	go func() {
		if err := fake.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case fake.done <- fmt.Errorf("serve fake app-server: %w", err):
			default:
			}
		}
	}()
	t.Cleanup(func() {
		_ = fake.server.Close()
		_ = fake.listener.Close()
	})
	return fake
}

func (s *fakeAppServer) wait(t *testing.T) {
	t.Helper()
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("fake app-server: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake app-server did not finish")
	}
}

func readFakeRequest(conn *websocket.Conn, wantMethod string) (fakeAppServerRequest, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var request fakeAppServerRequest
	if err := wsjson.Read(ctx, conn, &request); err != nil {
		return request, err
	}
	if request.Method != wantMethod {
		return request, fmt.Errorf("method = %q, want %q", request.Method, wantMethod)
	}
	return request, nil
}

func readFakeMessage(conn *websocket.Conn) (fakeAppServerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var message fakeAppServerMessage
	if err := wsjson.Read(ctx, conn, &message); err != nil {
		return message, err
	}
	return message, nil
}

func writeFakeMessage(conn *websocket.Conn, message any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return wsjson.Write(ctx, conn, message)
}

func fakeInitialize(conn *websocket.Conn) error {
	request, err := readFakeRequest(conn, "initialize")
	if err != nil {
		return err
	}
	if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": map[string]any{"userAgent": "fake"}}); err != nil {
		return err
	}
	_, err = readFakeRequest(conn, "initialized")
	return err
}

func fakeStartOrResume(conn *websocket.Conn, method, threadID string) (map[string]any, error) {
	request, err := readFakeRequest(conn, method)
	if err != nil {
		return nil, err
	}
	var params map[string]any
	if err := json.Unmarshal(request.Params, &params); err != nil {
		return nil, err
	}
	result := map[string]any{
		"thread":        map[string]any{"id": threadID, "sessionId": threadID},
		"model":         "gpt-5.4",
		"modelProvider": "openai",
		"serviceTier":   "priority",
		"cwd":           "/work/repo",
	}
	if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": result}); err != nil {
		return nil, err
	}
	return params, nil
}

func fakeSuccessfulTurn(conn *websocket.Conn, threadID, text string, beforeCompletion <-chan struct{}) (map[string]any, error) {
	request, err := readFakeRequest(conn, "turn/start")
	if err != nil {
		return nil, err
	}
	var params map[string]any
	if err := json.Unmarshal(request.Params, &params); err != nil {
		return nil, err
	}
	turnID := "turn-1"
	if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": map[string]any{
		"turn": map[string]any{"id": turnID, "status": "inProgress", "items": []any{}},
	}}); err != nil {
		return nil, err
	}
	if beforeCompletion != nil {
		select {
		case <-beforeCompletion:
		case <-time.After(5 * time.Second):
			return nil, errors.New("timed out waiting to release fake turn")
		}
	}
	messages := []any{
		map[string]any{"method": "item/started", "params": map[string]any{
			"threadId": threadID, "turnId": turnID, "item": map[string]any{
				"id": "cmd-1", "type": "commandExecution", "command": "go test ./internal/agent", "status": "inProgress",
			},
		}},
		map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{
			"threadId": threadID, "turnId": turnID, "itemId": "msg-1", "delta": text[:len(text)/2],
		}},
		map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{
			"threadId": threadID, "turnId": turnID, "itemId": "msg-1", "delta": text[len(text)/2:],
		}},
		map[string]any{"method": "item/completed", "params": map[string]any{
			"threadId": threadID, "turnId": turnID, "completedAtMs": 1, "item": map[string]any{
				"id": "cmd-1", "type": "commandExecution", "command": "go test ./internal/agent", "status": "completed",
			},
		}},
		map[string]any{"method": "item/completed", "params": map[string]any{
			"threadId": threadID, "turnId": turnID, "completedAtMs": 2, "item": map[string]any{
				"id": "msg-1", "type": "agentMessage", "text": text,
			},
		}},
		map[string]any{"method": "thread/tokenUsage/updated", "params": map[string]any{
			"threadId": threadID, "turnId": turnID, "tokenUsage": map[string]any{
				"total": map[string]any{"inputTokens": 120, "cachedInputTokens": 20, "cacheWriteInputTokens": 5, "outputTokens": 30, "reasoningOutputTokens": 7, "totalTokens": 150},
				"last":  map[string]any{"inputTokens": 120, "cachedInputTokens": 20, "cacheWriteInputTokens": 5, "outputTokens": 30, "reasoningOutputTokens": 7, "totalTokens": 150},
			},
		}},
		map[string]any{"method": "turn/completed", "params": map[string]any{
			"threadId": threadID, "turn": map[string]any{"id": turnID, "status": "completed", "items": []any{}},
		}},
	}
	for _, message := range messages {
		if err := writeFakeMessage(conn, message); err != nil {
			return nil, err
		}
	}
	return params, nil
}

func TestCodexAgent_AppServerStartStreamsStructuredResultAndMetrics(t *testing.T) {
	var threadParams, turnParams map[string]any
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		var err error
		threadParams, err = fakeStartOrResume(conn, "thread/start", "thread-new-123")
		if err != nil {
			return err
		}
		turnParams, err = fakeSuccessfulTurn(conn, "thread-new-123", `{"answer":"ok"}`, nil)
		return err
	})

	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)
	var chunks strings.Builder
	var lifecycle []LifecycleEvent
	var lifecycleMu sync.Mutex
	agent := &codexAgent{
		appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint},
		extraArgs: []string{
			"-m", "gpt-5.4", "-c", `service_tier="priority"`, "-c", `model_reasoning_effort="low"`,
		},
	}
	result, err := agent.Run(context.Background(), RunOpts{
		Prompt:     "review this change",
		CWD:        "/work/repo",
		JSONSchema: schema,
		Session:    &SessionRef{},
		OnChunk:    func(text string) { chunks.WriteString(text) },
		OnLifecycle: func(event LifecycleEvent) {
			lifecycleMu.Lock()
			lifecycle = append(lifecycle, event)
			lifecycleMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	server.wait(t)

	if result.SessionID != "thread-new-123" || result.Resumed {
		t.Fatalf("session result = id %q resumed %v", result.SessionID, result.Resumed)
	}
	if string(result.Output) != `{"answer":"ok"}` || result.Text != `{"answer":"ok"}` {
		t.Fatalf("structured result = output %s text %q", result.Output, result.Text)
	}
	if chunks.String() != `{"answer":"ok"}` {
		t.Fatalf("streamed chunks = %q", chunks.String())
	}
	if result.Usage.InputTokens != 120 || result.Usage.CacheReadTokens != 20 || result.Usage.CacheCreationTokens != 5 || result.Usage.OutputTokens != 30 || result.Usage.ReasoningTokens != 7 || !result.UsageReported || !result.CacheCreationReported {
		t.Fatalf("usage = %+v reported=%v", result.Usage, result.UsageReported)
	}
	if result.Model != "gpt-5.4" || result.ModelProvider != "openai" {
		t.Fatalf("model = %q provider = %q", result.Model, result.ModelProvider)
	}
	if result.Metrics == nil || result.Metrics.ModelRoundtrips != 2 || result.Metrics.ToolCalls != 1 || result.Metrics.ToolCategories.TestLint != 1 {
		t.Fatalf("metrics = %+v", result.Metrics)
	}
	if threadParams["cwd"] != "/work/repo" || threadParams["model"] != "gpt-5.4" || threadParams["approvalPolicy"] != "never" || threadParams["sandbox"] != "danger-full-access" || threadParams["approvalsReviewer"] != "user" {
		t.Fatalf("thread/start params = %#v", threadParams)
	}
	configValues, ok := threadParams["config"].(map[string]any)
	if !ok || configValues["service_tier"] != "priority" || configValues["model_reasoning_effort"] != "low" {
		t.Fatalf("thread/start config = %#v", threadParams["config"])
	}
	if _, present := configValues["project_doc_max_bytes"]; present {
		t.Fatalf("thread/start config must not claim incomplete project suppression: %#v", configValues)
	}
	if configValues["shell_environment_policy.set.NO_MISTAKES_GATE"] != "1" {
		t.Fatalf("thread/start gate environment = %#v", configValues)
	}
	for _, override := range gitinternal.NonInteractiveEnvOverrides() {
		if got := configValues["shell_environment_policy.set."+override.Name]; got != override.Value {
			t.Errorf("thread/start Git environment %s = %#v, owner requires %q", override.Name, got, override.Value)
		}
	}
	if turnParams["threadId"] != "thread-new-123" || turnParams["cwd"] != "/work/repo" || turnParams["approvalsReviewer"] != "user" {
		t.Fatalf("turn/start params = %#v", turnParams)
	}
	input, ok := turnParams["input"].([]any)
	if !ok || len(input) != 1 || input[0].(map[string]any)["text"] != "review this change" {
		t.Fatalf("turn/start input = %#v", turnParams["input"])
	}
	if turnParams["approvalPolicy"] != "never" || turnParams["sandboxPolicy"].(map[string]any)["type"] != "dangerFullAccess" {
		t.Fatalf("turn/start execution posture = %#v", turnParams)
	}
	if _, ok := turnParams["outputSchema"].(map[string]any); !ok {
		t.Fatalf("turn/start outputSchema = %#v", turnParams["outputSchema"])
	}
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	foundSession := false
	foundServerPID := false
	for _, event := range lifecycle {
		if event.Phase == LifecyclePhaseStart && event.PID > 0 {
			foundServerPID = true
		}
		if event.Phase == LifecyclePhaseSession && event.SessionID == "thread-new-123" {
			foundSession = true
		}
	}
	if !foundSession {
		t.Fatalf("lifecycle missing active thread identity: %+v", lifecycle)
	}
	if !foundServerPID {
		t.Fatalf("lifecycle missing authenticated app-server pid: %+v", lifecycle)
	}
}

func TestCodexAgent_AppServerResume(t *testing.T) {
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		params, err := fakeStartOrResume(conn, "thread/resume", "thread-existing")
		if err != nil {
			return err
		}
		if params["threadId"] != "thread-existing" {
			return fmt.Errorf("resume threadId = %#v", params["threadId"])
		}
		if params["approvalsReviewer"] != "user" {
			return fmt.Errorf("resume approvalsReviewer = %#v", params["approvalsReviewer"])
		}
		if _, ok := params["serviceName"]; ok {
			return fmt.Errorf("thread/resume received unsupported serviceName: %#v", params)
		}
		_, err = fakeSuccessfulTurn(conn, "thread-existing", "resumed", nil)
		return err
	})

	result, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(context.Background(), RunOpts{
		Prompt: "continue", CWD: "/work/repo", Session: &SessionRef{ID: "thread-existing", Agent: "codex"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	server.wait(t)
	if result.SessionID != "thread-existing" || !result.Resumed || result.Text != "resumed" {
		t.Fatalf("resume result = %+v", result)
	}
}

func TestCodexAgent_AppServerAcceptsLargeCompletedItems(t *testing.T) {
	largeText := strings.Repeat("x", 128*1024)
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		if _, err := fakeStartOrResume(conn, "thread/start", "thread-large"); err != nil {
			return err
		}
		_, err := fakeSuccessfulTurn(conn, "thread-large", largeText, nil)
		return err
	})

	result, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(context.Background(), RunOpts{
		Prompt: "produce a large result", CWD: "/work/repo", Session: &SessionRef{},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	server.wait(t)
	if result.Text != largeText {
		t.Fatalf("large result length = %d, want %d", len(result.Text), len(largeText))
	}
}

func TestCodexAgent_AppServerPublishesThreadIdentityBeforeCompletion(t *testing.T) {
	release := make(chan struct{})
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		if _, err := fakeStartOrResume(conn, "thread/start", "thread-live-early"); err != nil {
			return err
		}
		_, err := fakeSuccessfulTurn(conn, "thread-live-early", "done", release)
		return err
	})

	identity := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		_, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(context.Background(), RunOpts{
			Prompt: "work", CWD: "/work/repo", Session: &SessionRef{},
			OnLifecycle: func(event LifecycleEvent) {
				if event.Phase == LifecyclePhaseSession {
					identity <- event.SessionID
				}
			},
		})
		done <- err
	}()
	select {
	case got := <-identity:
		if got != "thread-live-early" {
			t.Fatalf("active identity = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("thread identity was not published while turn was active")
	}
	select {
	case err := <-done:
		t.Fatalf("invocation completed before release: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	server.wait(t)
}

func TestCodexAgent_AppServerCancellationInterruptsTurn(t *testing.T) {
	interruptSeen := make(chan map[string]any, 1)
	approvalSeen := make(chan string, 1)
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		if _, err := fakeStartOrResume(conn, "thread/start", "thread-cancel"); err != nil {
			return err
		}
		request, err := readFakeRequest(conn, "turn/start")
		if err != nil {
			return err
		}
		if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": map[string]any{
			"turn": map[string]any{"id": "turn-cancel", "status": "inProgress", "items": []any{}},
		}}); err != nil {
			return err
		}
		if err := writeFakeMessage(conn, map[string]any{
			"id": 81, "method": "item/commandExecution/requestApproval", "params": map[string]any{
				"threadId": "thread-cancel", "turnId": "turn-cancel", "itemId": "cmd-cancel",
			},
		}); err != nil {
			return err
		}
		gotApproval, gotInterrupt := false, false
		for !gotApproval || !gotInterrupt {
			message, err := readFakeMessage(conn)
			if err != nil {
				return err
			}
			switch {
			case message.Method == "turn/interrupt":
				var params map[string]any
				if err := json.Unmarshal(message.Params, &params); err != nil {
					return err
				}
				interruptSeen <- params
				gotInterrupt = true
				if err := writeFakeMessage(conn, map[string]any{"id": message.ID, "result": map[string]any{}}); err != nil {
					return err
				}
			case message.ID == 81:
				var result struct {
					Decision string `json:"decision"`
				}
				if err := json.Unmarshal(message.Result, &result); err != nil {
					return err
				}
				approvalSeen <- result.Decision
				gotApproval = true
			default:
				return fmt.Errorf("unexpected cancellation message: %+v", message)
			}
		}
		return writeFakeMessage(conn, map[string]any{"method": "turn/completed", "params": map[string]any{
			"threadId": "thread-cancel", "turn": map[string]any{"id": "turn-cancel", "status": "interrupted", "items": []any{}},
		}})
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(ctx, RunOpts{
			Prompt: "work", CWD: "/work/repo", Session: &SessionRef{},
			OnLifecycle: func(event LifecycleEvent) {
				if event.Phase == LifecyclePhaseSession {
					cancel()
				}
			},
		})
		done <- err
	}()
	select {
	case params := <-interruptSeen:
		if params["threadId"] != "thread-cancel" || params["turnId"] != "turn-cancel" {
			t.Fatalf("interrupt params = %#v", params)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("turn/interrupt was not sent")
	}
	select {
	case decision := <-approvalSeen:
		if decision != "decline" {
			t.Fatalf("approval decision = %q, want decline", decision)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("approval was not declined during cancellation")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", err)
	}
	server.wait(t)
}

func TestCodexAgent_AppServerIgnoresForeignNotifications(t *testing.T) {
	foreignSent := make(chan struct{})
	release := make(chan struct{})
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		if _, err := fakeStartOrResume(conn, "thread/start", "thread-owned"); err != nil {
			return err
		}
		request, err := readFakeRequest(conn, "turn/start")
		if err != nil {
			return err
		}
		if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": map[string]any{
			"turn": map[string]any{"id": "turn-owned", "status": "inProgress", "items": []any{}},
		}}); err != nil {
			return err
		}
		foreign := []any{
			map[string]any{"method": "item/started", "params": map[string]any{
				"threadId": "thread-foreign", "turnId": "turn-owned", "item": map[string]any{
					"id": "foreign-command", "type": "commandExecution", "command": "go test ./...",
				},
			}},
			map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{
				"threadId": "thread-owned", "turnId": "turn-foreign", "itemId": "foreign-message", "delta": "foreign delta",
			}},
			map[string]any{"method": "item/completed", "params": map[string]any{
				"threadId": "thread-foreign", "turnId": "turn-owned", "item": map[string]any{
					"id": "foreign-message", "type": "agentMessage", "text": "foreign result",
				},
			}},
			map[string]any{"method": "thread/tokenUsage/updated", "params": map[string]any{
				"threadId": "thread-owned", "turnId": "turn-foreign", "tokenUsage": map[string]any{
					"total": map[string]any{"inputTokens": 999, "outputTokens": 999},
				},
			}},
			map[string]any{"method": "error", "params": map[string]any{
				"threadId": "thread-foreign", "turnId": "turn-owned", "willRetry": false,
				"error": map[string]any{"message": "foreign failure"},
			}},
			map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": "thread-foreign", "turn": map[string]any{"id": "turn-owned", "status": "completed"},
			}},
			map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": "thread-owned", "turn": map[string]any{"id": "turn-foreign", "status": "completed"},
			}},
		}
		for _, message := range foreign {
			if err := writeFakeMessage(conn, message); err != nil {
				return err
			}
		}
		close(foreignSent)
		select {
		case <-release:
		case <-time.After(5 * time.Second):
			return errors.New("timed out waiting to send owned notifications")
		}
		owned := []any{
			map[string]any{"method": "item/completed", "params": map[string]any{
				"threadId": "thread-owned", "turnId": "turn-owned", "item": map[string]any{
					"id": "owned-message", "type": "agentMessage", "text": "owned result",
				},
			}},
			map[string]any{"method": "thread/tokenUsage/updated", "params": map[string]any{
				"threadId": "thread-owned", "turnId": "turn-owned", "tokenUsage": map[string]any{
					"total": map[string]any{"inputTokens": 12, "outputTokens": 3},
				},
			}},
			map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": "thread-owned", "turn": map[string]any{"id": "turn-owned", "status": "completed"},
			}},
		}
		for _, message := range owned {
			if err := writeFakeMessage(conn, message); err != nil {
				return err
			}
		}
		return nil
	})

	type runOutcome struct {
		result *Result
		err    error
	}
	var chunks strings.Builder
	done := make(chan runOutcome, 1)
	go func() {
		result, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(context.Background(), RunOpts{
			Prompt: "work", CWD: "/work/repo", Session: &SessionRef{}, OnChunk: func(text string) { chunks.WriteString(text) },
		})
		done <- runOutcome{result: result, err: err}
	}()
	select {
	case <-foreignSent:
	case <-time.After(3 * time.Second):
		t.Fatal("fake app-server did not send foreign notifications")
	}
	select {
	case outcome := <-done:
		t.Fatalf("foreign completion ended invocation: result=%+v err=%v", outcome.result, outcome.err)
	default:
	}
	close(release)
	outcome := <-done
	if outcome.err != nil {
		t.Fatalf("Run: %v", outcome.err)
	}
	server.wait(t)
	if outcome.result.Text != "owned result" || chunks.String() != "owned result" {
		t.Fatalf("owned output = result %q chunks %q", outcome.result.Text, chunks.String())
	}
	if outcome.result.Usage.InputTokens != 12 || outcome.result.Usage.OutputTokens != 3 {
		t.Fatalf("owned usage = %+v", outcome.result.Usage)
	}
	if outcome.result.Metrics == nil || outcome.result.Metrics.ModelRoundtrips != 1 || outcome.result.Metrics.ToolCalls != 0 {
		t.Fatalf("owned metrics = %+v", outcome.result.Metrics)
	}
}

func TestCodexAgent_AppServerReadFailureInterruptsTurnExactlyOnce(t *testing.T) {
	server := startFakeAppServer(t, func(conn *websocket.Conn) error {
		if err := fakeInitialize(conn); err != nil {
			return err
		}
		if _, err := fakeStartOrResume(conn, "thread/start", "thread-read-failure"); err != nil {
			return err
		}
		request, err := readFakeRequest(conn, "turn/start")
		if err != nil {
			return err
		}
		if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": map[string]any{
			"turn": map[string]any{"id": "turn-read-failure", "status": "inProgress", "items": []any{}},
		}}); err != nil {
			return err
		}
		interrupt, err := readFakeRequest(conn, "turn/interrupt")
		if err != nil {
			return err
		}
		var params map[string]any
		if err := json.Unmarshal(interrupt.Params, &params); err != nil {
			return err
		}
		if params["threadId"] != "thread-read-failure" || params["turnId"] != "turn-read-failure" {
			return fmt.Errorf("interrupt params = %#v", params)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var duplicate fakeAppServerMessage
		if err := wsjson.Read(ctx, conn, &duplicate); err == nil && duplicate.Method == "turn/interrupt" {
			return fmt.Errorf("duplicate turn/interrupt: %+v", duplicate)
		}
		return nil
	})

	agent := &codexAgent{
		appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint},
		appServerReadMessage: func(context.Context, *websocket.Conn, *codexAppRPCMessage) error {
			return io.EOF
		},
	}
	_, err := agent.runAppServerOnce(context.Background(), RunOpts{
		Prompt: "work", CWD: "/work/repo", Session: &SessionRef{},
	})
	if err == nil || !strings.Contains(err.Error(), "read events: EOF") {
		t.Fatalf("runAppServerOnce error = %v, want read-events EOF", err)
	}
	server.wait(t)
}

func TestCodexAgent_AppServerDeclinesNonInteractiveApprovals(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		wantResult map[string]any
	}{
		{
			name:       "command execution",
			method:     "item/commandExecution/requestApproval",
			wantResult: map[string]any{"decision": "decline"},
		},
		{
			name:       "file change",
			method:     "item/fileChange/requestApproval",
			wantResult: map[string]any{"decision": "decline"},
		},
		{
			name:   "permission escalation",
			method: "item/permissions/requestApproval",
			wantResult: map[string]any{
				"permissions": map[string]any{},
				"scope":       "turn",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := startFakeAppServer(t, func(conn *websocket.Conn) error {
				if err := fakeInitialize(conn); err != nil {
					return err
				}
				threadParams, err := fakeStartOrResume(conn, "thread/start", "thread-approval")
				if err != nil {
					return err
				}
				if threadParams["approvalPolicy"] != "on-request" || threadParams["sandbox"] != "workspace-write" || threadParams["approvalsReviewer"] != "user" {
					return fmt.Errorf("thread posture = %#v", threadParams)
				}
				config, ok := threadParams["config"].(map[string]any)
				if !ok || config["approvals_reviewer"] != "model" {
					return fmt.Errorf("thread inherited reviewer config = %#v", threadParams["config"])
				}
				turn, err := readFakeRequest(conn, "turn/start")
				if err != nil {
					return err
				}
				var turnParams map[string]any
				if err := json.Unmarshal(turn.Params, &turnParams); err != nil {
					return err
				}
				if turnParams["approvalPolicy"] != "on-request" || turnParams["approvalsReviewer"] != "user" {
					return fmt.Errorf("turn posture = %#v", turnParams)
				}
				if err := writeFakeMessage(conn, map[string]any{"id": turn.ID, "result": map[string]any{
					"turn": map[string]any{"id": "turn-approval", "status": "inProgress", "items": []any{}},
				}}); err != nil {
					return err
				}
				if err := writeFakeMessage(conn, map[string]any{
					"id": 92, "method": tt.method, "params": map[string]any{
						"threadId": "thread-approval", "turnId": "turn-approval", "itemId": "item-approval",
					},
				}); err != nil {
					return err
				}
				response, err := readFakeMessage(conn)
				if err != nil {
					return err
				}
				if response.ID != 92 || response.Error != nil {
					return fmt.Errorf("approval response = %+v", response)
				}
				var result map[string]any
				if err := json.Unmarshal(response.Result, &result); err != nil {
					return err
				}
				if !mapsEqualJSON(result, tt.wantResult) {
					return fmt.Errorf("approval result = %#v, want %#v", result, tt.wantResult)
				}
				if err := writeFakeMessage(conn, map[string]any{"method": "item/completed", "params": map[string]any{
					"threadId": "thread-approval", "turnId": "turn-approval", "item": map[string]any{
						"id": "msg-approval", "type": "agentMessage", "text": "continued safely",
					},
				}}); err != nil {
					return err
				}
				return writeFakeMessage(conn, map[string]any{"method": "turn/completed", "params": map[string]any{
					"threadId": "thread-approval", "turn": map[string]any{"id": "turn-approval", "status": "completed", "items": []any{}},
				}})
			})

			var lifecycle []LifecycleEvent
			result, err := (&codexAgent{
				appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint},
				extraArgs: []string{"--ask-for-approval", "on-request", "--sandbox", "workspace-write", "-c", `approvals_reviewer="model"`},
			}).Run(context.Background(), RunOpts{
				Prompt: "work", CWD: "/work/repo", Session: &SessionRef{},
				OnLifecycle: func(event LifecycleEvent) { lifecycle = append(lifecycle, event) },
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			server.wait(t)
			if result.Text != "continued safely" {
				t.Fatalf("result text = %q", result.Text)
			}
			wantActivity := "approval declined method=" + tt.method + " policy=on-request"
			found := false
			for _, event := range lifecycle {
				if event.Phase == LifecyclePhaseActivity && strings.Contains(event.Message, wantActivity) {
					found = true
				}
			}
			if !found {
				t.Fatalf("lifecycle missing %q: %+v", wantActivity, lifecycle)
			}
		})
	}
}

func mapsEqualJSON(got, want map[string]any) bool {
	gotJSON, gotErr := json.Marshal(got)
	wantJSON, wantErr := json.Marshal(want)
	return gotErr == nil && wantErr == nil && string(gotJSON) == string(wantJSON)
}

func TestCodexAgent_AppServerErrorPaths(t *testing.T) {
	t.Run("resume RPC error", func(t *testing.T) {
		server := startFakeAppServer(t, func(conn *websocket.Conn) error {
			if err := fakeInitialize(conn); err != nil {
				return err
			}
			request, err := readFakeRequest(conn, "thread/resume")
			if err != nil {
				return err
			}
			return writeFakeMessage(conn, map[string]any{"id": request.ID, "error": map[string]any{"code": -32000, "message": "thread missing"}})
		})
		_, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(context.Background(), RunOpts{
			Prompt: "continue", CWD: "/work/repo", Session: &SessionRef{ID: "gone"},
		})
		if err == nil || !strings.Contains(err.Error(), "thread missing") {
			t.Fatalf("Run error = %v", err)
		}
		server.wait(t)
	})

	t.Run("failed turn", func(t *testing.T) {
		server := startFakeAppServer(t, func(conn *websocket.Conn) error {
			if err := fakeInitialize(conn); err != nil {
				return err
			}
			if _, err := fakeStartOrResume(conn, "thread/start", "thread-fail"); err != nil {
				return err
			}
			request, err := readFakeRequest(conn, "turn/start")
			if err != nil {
				return err
			}
			if err := writeFakeMessage(conn, map[string]any{"id": request.ID, "result": map[string]any{
				"turn": map[string]any{"id": "turn-fail", "status": "inProgress", "items": []any{}},
			}}); err != nil {
				return err
			}
			if err := writeFakeMessage(conn, map[string]any{"method": "error", "params": map[string]any{
				"threadId": "thread-fail", "turnId": "turn-fail", "willRetry": false, "error": map[string]any{"message": "model exploded"},
			}}); err != nil {
				return err
			}
			return writeFakeMessage(conn, map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": "thread-fail", "turn": map[string]any{"id": "turn-fail", "status": "failed", "items": []any{}, "error": map[string]any{"message": "model exploded"}},
			}})
		})
		_, err := (&codexAgent{appServer: CodexAppServerOptions{Enabled: true, Endpoint: server.endpoint}}).Run(context.Background(), RunOpts{
			Prompt: "work", CWD: "/work/repo", Session: &SessionRef{},
		})
		if err == nil || !strings.Contains(err.Error(), "model exploded") {
			t.Fatalf("Run error = %v", err)
		}
		server.wait(t)
	})
}

func TestCodexAgent_DefaultTransportRemainsExec(t *testing.T) {
	created, err := NewWithOptions(types.AgentCodex, "codex", nil, Options{})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	codex, ok := created.(*codexAgent)
	if !ok {
		t.Fatalf("agent type = %T", created)
	}
	if codex.usesAppServer() {
		t.Fatal("default Codex transport unexpectedly uses app-server")
	}
}

func TestCodexAppServerSocketPath_DefaultUsesCodexHome(t *testing.T) {
	codexHome := t.TempDir()
	t.Setenv("CODEX_HOME", codexHome)

	got, err := codexAppServerSocketPath("unix://")
	if err != nil {
		t.Fatalf("codexAppServerSocketPath: %v", err)
	}
	want := filepath.Join(codexHome, "app-server-control", "app-server-control.sock")
	if got != want {
		t.Fatalf("default socket path = %q, want %q", got, want)
	}
}

func TestCodexAppServerOverrides_UsesDistinctThreadAndTurnSandboxEnums(t *testing.T) {
	tests := []struct {
		name       string
		extraArgs  []string
		threadMode string
		turnType   string
	}{
		{name: "default", threadMode: "danger-full-access", turnType: "dangerFullAccess"},
		{name: "read only", extraArgs: []string{"--sandbox", "read-only"}, threadMode: "read-only", turnType: "readOnly"},
		{name: "workspace write", extraArgs: []string{"--sandbox=workspace-write"}, threadMode: "workspace-write", turnType: "workspaceWrite"},
		{name: "danger full access", extraArgs: []string{"-s", "danger-full-access"}, threadMode: "danger-full-access", turnType: "dangerFullAccess"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			overrides, err := (&codexAgent{extraArgs: test.extraArgs}).appServerOverrides()
			if err != nil {
				t.Fatalf("appServerOverrides: %v", err)
			}
			thread := overrides.threadParams("/work/repo")
			if thread["sandbox"] != test.threadMode {
				t.Fatalf("thread sandbox = %#v, want %q", thread["sandbox"], test.threadMode)
			}
			if thread["approvalsReviewer"] != "user" {
				t.Fatalf("thread approvalsReviewer = %#v, want user", thread["approvalsReviewer"])
			}
			turn := overrides.turnParams("thread-1", "work", "/work/repo", nil)
			if turn["approvalsReviewer"] != "user" {
				t.Fatalf("turn approvalsReviewer = %#v, want user", turn["approvalsReviewer"])
			}
			policy, ok := turn["sandboxPolicy"].(map[string]any)
			if !ok || policy["type"] != test.turnType {
				t.Fatalf("turn sandbox policy = %#v, want type %q", turn["sandboxPolicy"], test.turnType)
			}
			if len(test.extraArgs) > 0 {
				if _, ok := thread["approvalPolicy"]; ok {
					t.Fatalf("partial override unexpectedly set approval policy: %#v", thread)
				}
			}
		})
	}
}

func TestCodexAppServerOverrides_UsesCanonicalNonInteractiveGitPosture(t *testing.T) {
	ag := &codexAgent{extraArgs: []string{
		"-c", `shell_environment_policy.set.GIT_EDITOR="false"`,
		"-c", `shell_environment_policy.set.NO_MISTAKES_GATE="0"`,
	}}
	overrides, err := ag.appServerOverrides()
	if err != nil {
		t.Fatalf("appServerOverrides: %v", err)
	}

	for _, override := range gitinternal.NonInteractiveEnvOverrides() {
		key := "shell_environment_policy.set." + override.Name
		if got := overrides.config[key]; got != override.Value {
			t.Errorf("App Server config %s = %#v, Git owner requires %q", key, got, override.Value)
		}
	}
	if got := overrides.config["shell_environment_policy.set."+GateRoleEnvVar]; got != "1" {
		t.Errorf("App Server gate marker = %#v, want 1", got)
	}
}

func TestCodexAppServerOverrides_RefusesProjectSettingsOptOut(t *testing.T) {
	ag := &codexAgent{disableProjectSettings: true}
	_, err := ag.appServerOverrides()
	if err == nil || !strings.Contains(err.Error(), "no per-thread equivalent") || !strings.Contains(err.Error(), "--ignore-rules") {
		t.Fatalf("appServerOverrides error = %v", err)
	}
}

func TestCodexAppServerOverrides_RejectsUnsupportedCLIFlag(t *testing.T) {
	_, err := (&codexAgent{extraArgs: []string{"--skip-git-repo-check"}}).appServerOverrides()
	if err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("appServerOverrides error = %v", err)
	}
}
