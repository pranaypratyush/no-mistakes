//go:build !linux && !darwin

package agent

import (
	"fmt"
	"net"
)

func codexAppServerPeerPID(net.Conn) (int, error) {
	return 0, fmt.Errorf("codex app-server transport requires authenticated Unix peer credentials (supported on Linux and macOS)")
}
