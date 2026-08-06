//go:build linux || darwin

package agent

import (
	"fmt"
	"net"
	"os"
	"syscall"
)

type codexAppServerPeerCredentials struct {
	PID  int
	EUID int
}

func codexAppServerPeerPID(conn net.Conn) (int, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("codex app-server unix connection does not expose peer credentials")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("codex app-server peer credentials: %w", err)
	}
	var credentials codexAppServerPeerCredentials
	var peerErr error
	if err := raw.Control(func(fd uintptr) {
		credentials, peerErr = codexAppServerPeerCredentialsForFD(int(fd))
	}); err != nil {
		return 0, fmt.Errorf("codex app-server peer credentials: %w", err)
	}
	if peerErr != nil {
		return 0, fmt.Errorf("codex app-server peer credentials: %w", peerErr)
	}
	return validateCodexAppServerPeerCredentials(credentials, os.Geteuid())
}

func validateCodexAppServerPeerCredentials(credentials codexAppServerPeerCredentials, effectiveUID int) (int, error) {
	if credentials.PID <= 0 {
		return 0, fmt.Errorf("codex app-server peer credentials returned no process id")
	}
	if effectiveUID < 0 {
		return 0, fmt.Errorf("codex app-server current effective user id is unavailable")
	}
	if credentials.EUID != effectiveUID {
		return 0, fmt.Errorf("codex app-server peer effective user id %d does not match current effective user id %d", credentials.EUID, effectiveUID)
	}
	return credentials.PID, nil
}
