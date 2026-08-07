package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/kunchenguid/no-mistakes/internal/buildinfo"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/pelletier/go-toml/v2"
)

const (
	defaultCodexAppServerEndpoint = "unix://"
	// Completed command items can carry captured output even though the adapter
	// extracts only bounded metrics from them. Raise coder/websocket's small
	// default while keeping a hard ceiling on one untrusted local frame.
	maxCodexAppServerMessageBytes  = 64 << 20
	codexAppServerInterruptTimeout = 2 * time.Second
)

// codexAppRPCMessage is the JSON-RPC-like envelope used by Codex App Server.
// IDs are kept raw because notifications omit them and the protocol permits
// both number and string IDs, while no-mistakes emits monotonically increasing
// numeric IDs.
type codexAppRPCMessage struct {
	ID     json.RawMessage   `json:"id,omitempty"`
	Method string            `json:"method,omitempty"`
	Params json.RawMessage   `json:"params,omitempty"`
	Result json.RawMessage   `json:"result,omitempty"`
	Error  *codexAppRPCError `json:"error,omitempty"`
}

type codexAppRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type codexAppServerReadFunc func(context.Context, *websocket.Conn, *codexAppRPCMessage) error

type codexAppClient struct {
	conn           *websocket.Conn
	opts           RunOpts
	approvalPolicy string
	nextID         int64
	mu             sync.Mutex
}

type codexAppThreadResponse struct {
	Thread struct {
		ID string `json:"id"`
	} `json:"thread"`
	Model         string `json:"model"`
	ModelProvider string `json:"modelProvider"`
}

type codexAppTurnResponse struct {
	Turn codexAppTurn `json:"turn"`
}

type codexAppTurn struct {
	ID     string             `json:"id"`
	Status string             `json:"status"`
	Items  []codexAppTurnItem `json:"items"`
}

type codexAppTurnItem struct {
	Type     string `json:"type"`
	ClientID string `json:"clientId"`
}

type codexAppThreadReadResponse struct {
	Thread struct {
		Turns []codexAppTurn `json:"turns"`
	} `json:"thread"`
}

type codexAppOverrides struct {
	model          string
	approvalPolicy string
	sandbox        string
	config         map[string]any
}

type codexAppRunState struct {
	opts          RunOpts
	threadID      string
	turnID        string
	metrics       *codexMetricsAccumulator
	usage         TokenUsage
	lastMessage   string
	deltaText     strings.Builder
	deltaItemSeen map[string]bool
	lastError     string
	completed     bool
	status        string
}

func (a *codexAgent) runAppServerOnce(ctx context.Context, opts RunOpts) (result *Result, retErr error) {
	endpoint := strings.TrimSpace(a.appServer.Endpoint)
	if endpoint == "" {
		endpoint = defaultCodexAppServerEndpoint
	}
	socketPath, err := codexAppServerSocketPath(endpoint)
	if err != nil {
		return nil, err
	}
	overrides, err := a.appServerOverrides()
	if err != nil {
		return nil, err
	}

	conn, serverPID, err := dialCodexAppServer(ctx, socketPath)
	if err != nil {
		return nil, fmt.Errorf("codex app-server connect: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	client := &codexAppClient{conn: conn, opts: opts, approvalPolicy: overrides.approvalPolicy}
	emitLifecycle(opts, LifecycleEvent{
		Agent: "codex", Phase: LifecyclePhaseStart, PID: serverPID,
		Message: fmt.Sprintf("codex app-server connected pid=%d", serverPID),
	})
	defer func() {
		message := "codex app-server invocation exited status=success"
		if retErr != nil {
			message = "codex app-server invocation exited error=" + retErr.Error()
		}
		emitLifecycle(opts, LifecycleEvent{Agent: "codex", Phase: LifecyclePhaseExit, Message: message})
	}()

	initializeParams := map[string]any{
		"clientInfo": map[string]any{
			"name":    "no_mistakes",
			"title":   "no-mistakes",
			"version": buildinfo.CurrentVersion(),
		},
	}
	if err := client.call(ctx, "initialize", initializeParams, nil); err != nil {
		return nil, fmt.Errorf("codex app-server initialize: %w", err)
	}
	if err := client.notify(ctx, "initialized", map[string]any{}); err != nil {
		return nil, fmt.Errorf("codex app-server initialized notification: %w", err)
	}

	resumeID := ""
	if opts.Session != nil {
		resumeID = strings.TrimSpace(opts.Session.ID)
	}
	threadMethod := "thread/start"
	threadParams := overrides.threadParams(opts.CWD)
	if resumeID != "" {
		threadMethod = "thread/resume"
		delete(threadParams, "serviceName")
		threadParams["threadId"] = resumeID
	}
	var thread codexAppThreadResponse
	if err := client.call(ctx, threadMethod, threadParams, &thread); err != nil {
		return nil, fmt.Errorf("codex app-server %s: %w", threadMethod, err)
	}
	threadID := strings.TrimSpace(thread.Thread.ID)
	if threadID == "" {
		return nil, fmt.Errorf("codex app-server %s returned no thread id", threadMethod)
	}

	validationSchema := opts.JSONSchema
	var outputSchema any
	if len(opts.JSONSchema) > 0 {
		normalized, err := codexOutputSchema(opts.JSONSchema)
		if err != nil {
			return nil, fmt.Errorf("codex schema normalize: %w", err)
		}
		validationSchema = normalized
		if err := json.Unmarshal(normalized, &outputSchema); err != nil {
			return nil, fmt.Errorf("codex app-server output schema: %w", err)
		}
	}
	turnParams := overrides.turnParams(threadID, opts.Prompt, opts.CWD, outputSchema)
	var turn codexAppTurnResponse
	if err := client.startTurn(ctx, threadID, turnParams, &turn); err != nil {
		return nil, fmt.Errorf("codex app-server turn/start: %w", err)
	}
	turnID := strings.TrimSpace(turn.Turn.ID)
	if turnID == "" {
		return nil, errors.New("codex app-server turn/start returned no turn id")
	}

	state := &codexAppRunState{
		opts:          opts,
		threadID:      threadID,
		turnID:        turnID,
		metrics:       newCodexMetricsAccumulator(),
		deltaItemSeen: map[string]bool{},
	}
	readMessage := a.appServerReadMessage
	if readMessage == nil {
		readMessage = readCodexAppServerMessage
	}
	var interruptOnce sync.Once
	interruptTurn := func() {
		interruptOnce.Do(func() {
			interruptCtx, cancel := context.WithTimeout(context.Background(), codexAppServerInterruptTimeout)
			defer cancel()
			_ = client.requestNoRead(interruptCtx, "turn/interrupt", map[string]any{
				"threadId": threadID,
				"turnId":   turnID,
			})
		})
	}

	readCtx, stopRead := context.WithCancel(context.Background())
	watchDone := make(chan struct{})
	watchExited := make(chan struct{})
	defer func() {
		close(watchDone)
		stopRead()
		<-watchExited
		// A terminal notification proves the server-side turn is already done.
		// Every other exit after turn/start gets one bounded best-effort cleanup
		// request while the websocket is still open.
		if !state.completed {
			interruptTurn()
		}
	}()
	go func() {
		defer close(watchExited)
		select {
		case <-ctx.Done():
			interruptTurn()
			// Let App Server acknowledge and publish turn/completed, but never let
			// a broken endpoint make cancellation hang indefinitely.
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				stopRead()
			case <-watchDone:
			}
		case <-watchDone:
		}
	}()
	// Publish only after the turn exists and its cancellation cleanup is armed,
	// so an observer can attach to this exact live thread without opening a gap
	// where a blocking lifecycle sink leaves an accepted turn uninterruptible.
	emitAgentSession(opts, "codex", threadID)

	for !state.completed {
		var message codexAppRPCMessage
		if err := readMessage(readCtx, conn, &message); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("codex app-server read events: %w", err)
		}
		if message.Method == "" {
			continue // response to turn/interrupt or another client-local request
		}
		if codexAppMessageHasID(message) {
			if err := client.handleServerRequest(ctx, message); err != nil {
				return nil, fmt.Errorf("codex app-server respond to %s: %w", message.Method, err)
			}
			continue
		}
		if err := state.handleNotification(message); err != nil {
			return nil, fmt.Errorf("codex app-server %s: %w", message.Method, err)
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if state.status != "completed" {
		detail := strings.TrimSpace(state.lastError)
		if detail == "" {
			detail = state.status
		}
		return nil, fmt.Errorf("codex app-server turn %s: %s", state.status, detail)
	}

	text := state.lastMessage
	if text == "" {
		text = state.deltaText.String()
	}
	result, err = finalizeTextResult("codex", text, validationSchema, state.usage)
	if result != nil {
		result.SessionID = threadID
		result.Resumed = resumeID != ""
		result.SessionUsageCumulative = true
		result.Model = sanitizeModelToken(thread.Model)
		result.ModelProvider = sanitizeModelToken(thread.ModelProvider)
		m := state.metrics.metrics()
		result.Metrics = &m
	}
	return result, err
}

func readCodexAppServerMessage(ctx context.Context, conn *websocket.Conn, message *codexAppRPCMessage) error {
	return wsjson.Read(ctx, conn, message)
}

func dialCodexAppServer(ctx context.Context, socketPath string) (*websocket.Conn, int, error) {
	dialer := &net.Dialer{}
	serverPID := 0
	var peerErr error
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, "unix", socketPath)
			if err != nil {
				return nil, err
			}
			serverPID, peerErr = codexAppServerPeerPID(conn)
			if peerErr != nil {
				_ = conn.Close()
				return nil, peerErr
			}
			return conn, nil
		},
	}
	client := &http.Client{Transport: transport}
	conn, _, err := websocket.Dial(ctx, "ws://localhost/rpc", &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, 0, err
	}
	if serverPID <= 0 {
		_ = conn.Close(websocket.StatusPolicyViolation, "unverified local peer")
		return nil, 0, errors.New("could not authenticate local app-server process")
	}
	conn.SetReadLimit(maxCodexAppServerMessageBytes)
	return conn, serverPID, nil
}

func codexAppServerSocketPath(endpoint string) (string, error) {
	if endpoint == "" || endpoint == defaultCodexAppServerEndpoint {
		codexHome := strings.TrimSpace(os.Getenv("CODEX_HOME"))
		if codexHome == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("resolve Codex home: %w", err)
			}
			codexHome = filepath.Join(home, ".codex")
		}
		return filepath.Join(codexHome, "app-server-control", "app-server-control.sock"), nil
	}
	if !strings.HasPrefix(endpoint, "unix://") {
		return "", fmt.Errorf("codex app-server endpoint must use unix://, got %q", endpoint)
	}
	rawPath := strings.TrimPrefix(endpoint, "unix://")
	decoded, err := url.PathUnescape(rawPath)
	if err != nil {
		return "", fmt.Errorf("decode codex app-server endpoint: %w", err)
	}
	if !filepath.IsAbs(decoded) {
		return "", fmt.Errorf("codex app-server socket path must be absolute, got %q", decoded)
	}
	return decoded, nil
}

func (c *codexAppClient) call(ctx context.Context, method string, params any, result any) error {
	id := c.nextRequestID()
	if err := c.write(ctx, map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	return c.readResponse(ctx, id, result)
}

// startTurn keeps the response read alive for one bounded cleanup window after
// cancellation. It also tags the user message and requests a turn-inclusive
// thread snapshot on cancellation, so an endpoint that accepted the turn but
// lost/delayed its direct response can still be reconciled without guessing at
// another client's active turn.
func (c *codexAppClient) startTurn(ctx context.Context, threadID string, params map[string]any, result *codexAppTurnResponse) error {
	clientMessageID, err := newCodexAppClientMessageID()
	if err != nil {
		return err
	}
	params["clientUserMessageId"] = clientMessageID
	startID := c.nextRequestID()
	if err := c.write(ctx, map[string]any{"id": startID, "method": "turn/start", "params": params}); err != nil {
		return err
	}
	reconcileID := c.nextRequestID()
	reconcileCtx, stopReconcile := contextWithCancellationGrace(ctx, codexAppServerInterruptTimeout, func(cleanupCtx context.Context) {
		_ = c.write(cleanupCtx, map[string]any{
			"id":     reconcileID,
			"method": "thread/read",
			"params": map[string]any{"threadId": threadID, "includeTurns": true},
		})
	})
	defer stopReconcile()
	if err := c.readTurnStartResponse(reconcileCtx, startID, reconcileID, clientMessageID, result); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: reconcile accepted turn/start: %v", ctx.Err(), err)
		}
		return err
	}
	return nil
}

func newCodexAppClientMessageID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate turn/start client message id: %w", err)
	}
	return "no-mistakes-" + hex.EncodeToString(random[:]), nil
}

func (c *codexAppClient) readTurnStartResponse(ctx context.Context, startID, reconcileID int64, clientMessageID string, result *codexAppTurnResponse) error {
	wantStartID := strconv.FormatInt(startID, 10)
	wantReconcileID := strconv.FormatInt(reconcileID, 10)
	for {
		var message codexAppRPCMessage
		if err := wsjson.Read(ctx, c.conn, &message); err != nil {
			return err
		}
		if message.Method != "" {
			if codexAppMessageHasID(message) {
				if err := c.handleServerRequest(ctx, message); err != nil {
					return err
				}
			}
			continue
		}
		switch string(message.ID) {
		case wantStartID:
			return decodeCodexAppResponse(message, result)
		case wantReconcileID:
			var snapshot codexAppThreadReadResponse
			if err := decodeCodexAppResponse(message, &snapshot); err != nil {
				continue // the exact turn/start response may still arrive
			}
			if turn, ok := findCodexAppTurnByClientMessageID(snapshot.Thread.Turns, clientMessageID); ok {
				result.Turn = turn
				return nil
			}
		}
	}
}

func findCodexAppTurnByClientMessageID(turns []codexAppTurn, clientMessageID string) (codexAppTurn, bool) {
	for _, turn := range turns {
		for _, item := range turn.Items {
			if item.Type == "userMessage" && item.ClientID == clientMessageID {
				return turn, true
			}
		}
	}
	return codexAppTurn{}, false
}

// contextWithCancellationGrace preserves parent values but delays its
// cancellation by grace. stop always joins the watcher, so the bounded cleanup
// window cannot leave a goroutine behind after the request completes.
func contextWithCancellationGrace(parent context.Context, grace time.Duration, onCancel func(context.Context)) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-parent.Done():
			graceCtx, cancelGrace := context.WithTimeout(context.WithoutCancel(parent), grace)
			defer cancelGrace()
			if onCancel != nil {
				onCancel(graceCtx)
			}
			select {
			case <-graceCtx.Done():
				cancel()
			case <-stop:
			}
		case <-stop:
		}
	}()
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			close(stop)
			<-exited
			cancel()
		})
	}
}

func (c *codexAppClient) readResponse(ctx context.Context, id int64, result any) error {
	wantID := strconv.FormatInt(id, 10)
	for {
		var message codexAppRPCMessage
		if err := wsjson.Read(ctx, c.conn, &message); err != nil {
			return err
		}
		if message.Method != "" {
			if codexAppMessageHasID(message) {
				if err := c.handleServerRequest(ctx, message); err != nil {
					return err
				}
			}
			continue
		}
		if string(message.ID) != wantID {
			continue
		}
		return decodeCodexAppResponse(message, result)
	}
}

func decodeCodexAppResponse(message codexAppRPCMessage, result any) error {
	if message.Error != nil {
		return fmt.Errorf("rpc error %d: %s", message.Error.Code, message.Error.Message)
	}
	if result != nil && len(message.Result) > 0 {
		if err := json.Unmarshal(message.Result, result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func codexAppMessageHasID(message codexAppRPCMessage) bool {
	id := strings.TrimSpace(string(message.ID))
	return id != "" && id != "null"
}

// handleServerRequest resolves every server-initiated request explicitly so
// the App Server's bidirectional RPC cannot wedge no-mistakes' single-reader
// loop. Gate invocations have no interactive approver: approval requests are
// therefore denied, never auto-approved, regardless of whether the configured
// posture is inherited, on-request, or untrusted. This preserves the selected
// server policy without turning it into an implicit privilege escalation.
func (c *codexAppClient) handleServerRequest(ctx context.Context, message codexAppRPCMessage) error {
	response := map[string]any{"id": message.ID}
	approval := false
	switch message.Method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		response["result"] = map[string]any{"decision": "decline"}
		approval = true
	case "item/permissions/requestApproval":
		// App Server treats omitted requested permissions as denied. Returning
		// an empty, turn-scoped subset is the protocol's fail-closed response.
		response["result"] = map[string]any{
			"permissions": map[string]any{},
			"scope":       "turn",
		}
		approval = true
	case "applyPatchApproval", "execCommandApproval":
		// Legacy approval methods are not expected for turn/start, but answer
		// them defensively so a mixed-version server cannot block indefinitely.
		response["result"] = map[string]any{"decision": map[string]any{
			"denied": map[string]any{"rejection": "no-mistakes runs non-interactively"},
		}}
		approval = true
	case "mcpServer/elicitation/request":
		response["result"] = map[string]any{"action": "decline", "content": nil}
	default:
		response["error"] = codexAppRPCError{
			Code:    -32601,
			Message: "no-mistakes does not support this server request",
		}
	}

	// A cancellation may race an approval prompt. Give the denial a short,
	// independent write budget while the cancellation watcher interrupts the
	// turn, so neither path can leave the server waiting on this request.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := c.write(writeCtx, response); err != nil {
		return err
	}
	if approval {
		policy := strings.TrimSpace(c.approvalPolicy)
		if policy == "" {
			policy = "inherited"
		}
		emitAgentActivity(c.opts, "codex", "codex app-server approval declined method="+message.Method+" policy="+policy)
	}
	return nil
}

func (c *codexAppClient) notify(ctx context.Context, method string, params any) error {
	return c.write(ctx, map[string]any{"method": method, "params": params})
}

// requestNoRead sends a request from the cancellation watcher. Its response is
// consumed by the invocation's single read loop, preserving the websocket's
// one-reader invariant.
func (c *codexAppClient) requestNoRead(ctx context.Context, method string, params any) error {
	id := c.nextRequestID()
	return c.write(ctx, map[string]any{"id": id, "method": method, "params": params})
}

func (c *codexAppClient) nextRequestID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	return c.nextID
}

func (c *codexAppClient) write(ctx context.Context, value any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return wsjson.Write(ctx, c.conn, value)
}

func (a *codexAgent) appServerOverrides() (codexAppOverrides, error) {
	o := codexAppOverrides{config: map[string]any{}}
	if a.disableProjectSettings {
		return o, errors.New("codex app-server transport cannot honor disable_project_settings: " +
			"the App Server protocol has no per-thread equivalent of codex exec --ignore-rules")
	}
	args := a.extraArgs
	for i := 0; i < len(args); i++ {
		arg := args[i]
		next := func(flag string) (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("codex app-server transport: %s requires a value", flag)
			}
			i++
			return args[i], nil
		}
		switch {
		case arg == "-m" || arg == "--model":
			value, err := next(arg)
			if err != nil {
				return o, err
			}
			o.model = value
		case strings.HasPrefix(arg, "--model="):
			o.model = strings.TrimPrefix(arg, "--model=")
		case arg == "-c" || arg == "--config":
			value, err := next(arg)
			if err != nil {
				return o, err
			}
			if err := setCodexAppConfigOverride(o.config, value); err != nil {
				return o, err
			}
		case strings.HasPrefix(arg, "--config="):
			if err := setCodexAppConfigOverride(o.config, strings.TrimPrefix(arg, "--config=")); err != nil {
				return o, err
			}
		case arg == "--enable" || arg == "--disable":
			value, err := next(arg)
			if err != nil {
				return o, err
			}
			o.config["features."+value] = arg == "--enable"
		case arg == "--ask-for-approval":
			value, err := next(arg)
			if err != nil {
				return o, err
			}
			o.approvalPolicy = value
		case strings.HasPrefix(arg, "--ask-for-approval="):
			o.approvalPolicy = strings.TrimPrefix(arg, "--ask-for-approval=")
		case arg == "-s" || arg == "--sandbox":
			value, err := next(arg)
			if err != nil {
				return o, err
			}
			o.sandbox = value
		case strings.HasPrefix(arg, "--sandbox="):
			o.sandbox = strings.TrimPrefix(arg, "--sandbox=")
		case arg == "--dangerously-bypass-approvals-and-sandbox":
			o.approvalPolicy = "never"
			o.sandbox = "danger-full-access"
		default:
			return o, fmt.Errorf("codex app-server transport does not support agent_args_override flag %q", arg)
		}
	}
	if o.approvalPolicy == "" && o.sandbox == "" {
		o.approvalPolicy = "never"
		o.sandbox = "danger-full-access"
	}
	// App Server is a shared parent process, so per-invocation environment
	// posture is carried through Codex's shell environment policy. Use the
	// same non-interactive Git owner as native subprocesses, then write the
	// recursive-gate marker after operator overrides so transport selection
	// cannot accidentally weaken containment.
	for _, override := range git.NonInteractiveEnvOverrides() {
		o.config["shell_environment_policy.set."+override.Name] = override.Value
	}
	o.config["shell_environment_policy.set."+GateRoleEnvVar] = "1"
	return o, nil
}

func setCodexAppConfigOverride(config map[string]any, override string) error {
	key, rawValue, ok := strings.Cut(override, "=")
	key = strings.TrimSpace(key)
	if !ok || key == "" {
		return fmt.Errorf("codex app-server config override must be key=value, got %q", override)
	}
	value := any(strings.TrimSpace(rawValue))
	var parsed map[string]any
	if err := toml.Unmarshal([]byte("value = "+rawValue+"\n"), &parsed); err == nil {
		value = parsed["value"]
	}
	config[key] = value
	return nil
}

func (o codexAppOverrides) threadParams(cwd string) map[string]any {
	params := map[string]any{
		"cwd":               cwd,
		"serviceName":       "no_mistakes",
		"approvalsReviewer": "user",
	}
	if o.approvalPolicy != "" {
		params["approvalPolicy"] = o.approvalPolicy
	}
	if o.sandbox != "" {
		// Thread RPCs use the CLI-style SandboxMode enum. turn/start's
		// sandboxPolicy is a different type with camelCase tags.
		params["sandbox"] = o.sandbox
	}
	if o.model != "" {
		params["model"] = o.model
	}
	if len(o.config) > 0 {
		params["config"] = o.config
	}
	return params
}

func (o codexAppOverrides) turnParams(threadID, prompt, cwd string, outputSchema any) map[string]any {
	params := map[string]any{
		"threadId":          threadID,
		"input":             []any{map[string]any{"type": "text", "text": prompt}},
		"cwd":               cwd,
		"approvalsReviewer": "user",
	}
	if o.approvalPolicy != "" {
		params["approvalPolicy"] = o.approvalPolicy
	}
	if o.sandbox != "" {
		params["sandboxPolicy"] = map[string]any{"type": codexAppTurnSandboxWireName(o.sandbox)}
	}
	if o.model != "" {
		params["model"] = o.model
	}
	if effort, ok := o.config["model_reasoning_effort"].(string); ok && effort != "" {
		params["effort"] = effort
	}
	if outputSchema != nil {
		params["outputSchema"] = outputSchema
	}
	return params
}

func codexAppTurnSandboxWireName(value string) string {
	switch value {
	case "read-only":
		return "readOnly"
	case "workspace-write":
		return "workspaceWrite"
	case "danger-full-access":
		return "dangerFullAccess"
	default:
		return value
	}
}

func (s *codexAppRunState) handleNotification(message codexAppRPCMessage) error {
	switch message.Method {
	case "item/started", "item/completed", "item/agentMessage/delta", "thread/tokenUsage/updated", "error", "turn/completed":
	default:
		return nil
	}
	matches, err := s.notificationMatchesInvocation(message)
	if err != nil {
		return err
	}
	if !matches {
		return nil
	}

	switch message.Method {
	case "item/started", "item/completed":
		var params struct {
			Item struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Text    string `json:"text"`
				Command string `json:"command"`
			} `json:"item"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		item := &codexItem{
			ID: params.Item.ID, Type: codexAppItemType(params.Item.Type),
			Text: params.Item.Text, Command: params.Item.Command,
		}
		eventType := "item.started"
		if message.Method == "item/completed" {
			eventType = "item.completed"
		}
		s.metrics.onItem(eventType, item, time.Now())
		if message.Method == "item/started" {
			emitAgentActivity(s.opts, "codex", "codex app-server item started type="+item.Type)
			return nil
		}
		if item.Type == "agent_message" {
			s.lastMessage = item.Text
			if !s.deltaItemSeen[item.ID] && s.opts.OnChunk != nil {
				s.opts.OnChunk(item.Text)
			}
		}
	case "item/agentMessage/delta":
		var params struct {
			ItemID string `json:"itemId"`
			Delta  string `json:"delta"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		s.deltaItemSeen[params.ItemID] = true
		s.deltaText.WriteString(params.Delta)
		if s.opts.OnChunk != nil {
			s.opts.OnChunk(params.Delta)
		}
	case "thread/tokenUsage/updated":
		var params struct {
			TokenUsage struct {
				Total struct {
					InputTokens           int  `json:"inputTokens"`
					CachedInputTokens     int  `json:"cachedInputTokens"`
					CacheWriteInputTokens *int `json:"cacheWriteInputTokens"`
					OutputTokens          int  `json:"outputTokens"`
					ReasoningOutputTokens int  `json:"reasoningOutputTokens"`
				} `json:"total"`
			} `json:"tokenUsage"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		total := params.TokenUsage.Total
		cacheCreationTokens := 0
		if total.CacheWriteInputTokens != nil {
			cacheCreationTokens = *total.CacheWriteInputTokens
		}
		s.usage = TokenUsage{
			InputTokens: total.InputTokens, CacheReadTokens: total.CachedInputTokens,
			CacheCreationTokens: cacheCreationTokens, OutputTokens: total.OutputTokens,
			ReasoningTokens: total.ReasoningOutputTokens, Reported: true,
			CacheCreationReported: total.CacheWriteInputTokens != nil,
		}
	case "error":
		var params struct {
			WillRetry bool `json:"willRetry"`
			Error     struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		if !params.WillRetry && params.Error.Message != "" {
			s.lastError = params.Error.Message
		}
	case "turn/completed":
		var params struct {
			Turn struct {
				Status string `json:"status"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			} `json:"turn"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		s.status = params.Turn.Status
		if params.Turn.Error != nil && params.Turn.Error.Message != "" {
			s.lastError = params.Turn.Error.Message
		}
		s.completed = true
	}
	return nil
}

func (s *codexAppRunState) notificationMatchesInvocation(message codexAppRPCMessage) (bool, error) {
	var identity struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Turn     struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := json.Unmarshal(message.Params, &identity); err != nil {
		return false, err
	}
	turnID := identity.TurnID
	if message.Method == "turn/completed" {
		turnID = identity.Turn.ID
	}
	return identity.ThreadID == s.threadID && turnID == s.turnID, nil
}

func codexAppItemType(value string) string {
	switch value {
	case "agentMessage":
		return "agent_message"
	case "commandExecution":
		return "command_execution"
	case "fileChange":
		return "file_change"
	case "mcpToolCall":
		return "mcp_tool_call"
	case "dynamicToolCall", "collabAgentToolCall", "imageView", "imageGeneration":
		return "custom_tool_call"
	case "webSearch":
		return "web_search"
	default:
		return value
	}
}
