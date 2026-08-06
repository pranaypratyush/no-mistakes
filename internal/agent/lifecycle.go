package agent

import "fmt"

const (
	// LifecyclePhaseStart marks native subprocess startup.
	LifecyclePhaseStart = "start"
	// LifecyclePhaseExit marks native subprocess exit.
	LifecyclePhaseExit = "exit"
	// LifecyclePhaseRetry marks a transient retry before the next subprocess attempt.
	LifecyclePhaseRetry = "retry"
	// LifecyclePhaseSession publishes a durable native session identity as soon
	// as it exists, while the invocation is still active.
	LifecyclePhaseSession = "session"
	// LifecyclePhaseActivity marks native non-process activity used for step
	// heartbeats and bounded lifecycle diagnostics.
	LifecyclePhaseActivity = "activity"
)

func emitAgentStarted(opts RunOpts, name string, pid int) {
	emitLifecycle(opts, LifecycleEvent{
		Agent:   name,
		Phase:   LifecyclePhaseStart,
		PID:     pid,
		Message: fmt.Sprintf("%s started pid=%d", name, pid),
	})
}

func emitAgentSession(opts RunOpts, name, sessionID string) {
	emitLifecycle(opts, LifecycleEvent{
		Agent:     name,
		Phase:     LifecyclePhaseSession,
		SessionID: sessionID,
		Message:   fmt.Sprintf("%s active session=%s", name, sessionID),
	})
}

func emitAgentActivity(opts RunOpts, name, message string) {
	emitLifecycle(opts, LifecycleEvent{
		Agent:   name,
		Phase:   LifecyclePhaseActivity,
		Message: message,
	})
}

func emitAgentExited(opts RunOpts, name string, pid int, err error) {
	message := fmt.Sprintf("%s exited pid=%d status=success", name, pid)
	if err != nil {
		message = fmt.Sprintf("%s exited pid=%d error=%s", name, pid, err.Error())
	}
	emitLifecycle(opts, LifecycleEvent{
		Agent:   name,
		Phase:   LifecyclePhaseExit,
		PID:     pid,
		Message: message,
	})
}

func emitAgentRetry(opts RunOpts, name string, label string, attempt, max int) {
	message := fmt.Sprintf("%s retrying after transient error %q (attempt %d/%d)", name, label, attempt, max)
	if opts.OnLifecycle != nil {
		emitLifecycle(opts, LifecycleEvent{
			Agent:   name,
			Phase:   LifecyclePhaseRetry,
			Message: message,
		})
		return
	}
	if opts.OnChunk != nil {
		opts.OnChunk(message)
	}
}

func emitLifecycle(opts RunOpts, event LifecycleEvent) {
	if opts.OnLifecycle != nil {
		opts.OnLifecycle(event)
	}
}
