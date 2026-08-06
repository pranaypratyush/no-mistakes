package git

import (
	"os"
	"path/filepath"
	"runtime"
)

// EnvOverride is one environment variable assignment in a subprocess posture.
type EnvOverride struct {
	Name  string
	Value string
}

var nonInteractiveEnvOverrides = [...]EnvOverride{
	{Name: "GIT_EDITOR", Value: "true"},
	{Name: "GIT_SEQUENCE_EDITOR", Value: "true"},
	{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
	// Read-only commands such as status and rev-parse must not refresh the
	// index as a side effect. Mutating commands still take required locks.
	{Name: "GIT_OPTIONAL_LOCKS", Value: "0"},
}

// NonInteractiveEnvOverrides returns the ordered Git environment assignments
// applied by NonInteractiveEnv. The returned slice is a copy so transports
// that cannot assign cmd.Env can render the same posture without mutating its
// process-wide owner.
func NonInteractiveEnvOverrides() []EnvOverride {
	return append([]EnvOverride(nil), nonInteractiveEnvOverrides[:]...)
}

// NonInteractiveEnv returns the environment for a subprocess that may invoke
// git, with git forced into a fully non-interactive mode. It is intended for
// cmd.Env on any subprocess that may run git (our own git calls and the coding
// agents we spawn).
//
// Without these overrides, git operations such as `git rebase --continue` or
// `git commit` open $EDITOR to confirm a commit message, and remote operations
// can block on a credential prompt. In a headless agent subprocess there is no
// TTY, so the editor or prompt hangs until the agent times out. Pointing the
// editors at `true` makes git accept the existing message immediately, and
// GIT_TERMINAL_PROMPT=0 fails fast instead of blocking on credentials. The
// overrides are appended last so they win over any ambient values (exec
// resolves duplicate keys using the last occurrence).
//
// Pass the same directory assigned to cmd.Dir (or "" when it is unset). When
// cmd.Env is left nil, os/exec injects PWD=cmd.Dir automatically; assigning
// cmd.Env disables that, so callers must thread the working directory through
// here to preserve symlinked working-directory paths (for example /tmp vs
// /private/tmp on macOS, which os.Getwd reports differently depending on PWD).
func NonInteractiveEnv(dir string) []string {
	return NonInteractiveEnvFrom(os.Environ(), dir)
}

// NonInteractiveEnvFrom is NonInteractiveEnv applied to an explicit base
// environment. A nil base means the current process environment.
func NonInteractiveEnvFrom(base []string, dir string) []string {
	if base == nil {
		base = os.Environ()
	}
	env := append([]string(nil), base...)
	for _, override := range nonInteractiveEnvOverrides {
		env = append(env, override.Name+"="+override.Value)
	}
	// Mirror os/exec, which only injects PWD when Cmd.Env is nil, skips it on
	// these platforms, and absolutizes Cmd.Dir first (go.dev/issue/50599):
	// POSIX defines PWD as "an absolute pathname of the current working
	// directory". Injecting a relative dir verbatim (for example ".") poisons
	// every descendant that trusts PWD — macOS /bin/sh is bash 3.2, whose pwd
	// builtin reports "." when PWD="." leaks through git receive-pack into a
	// hook, which is how the post-receive hook of issue #269 ended up passing
	// `--gate .`.
	if dir != "" && runtime.GOOS != "windows" && runtime.GOOS != "plan9" {
		if abs, err := filepath.Abs(dir); err == nil {
			env = append(env, "PWD="+abs)
		}
	}
	return env
}
