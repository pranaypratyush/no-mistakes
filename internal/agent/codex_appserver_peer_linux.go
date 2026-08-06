//go:build linux

package agent

import (
	"golang.org/x/sys/unix"
)

func codexAppServerPeerCredentialsForFD(fd int) (codexAppServerPeerCredentials, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return codexAppServerPeerCredentials{}, err
	}
	if cred == nil {
		return codexAppServerPeerCredentials{}, nil
	}
	return codexAppServerPeerCredentials{PID: int(cred.Pid), EUID: int(cred.Uid)}, nil
}
