//go:build darwin

package agent

import (
	"golang.org/x/sys/unix"
)

func codexAppServerPeerCredentialsForFD(fd int) (codexAppServerPeerCredentials, error) {
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return codexAppServerPeerCredentials{}, err
	}
	cred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return codexAppServerPeerCredentials{}, err
	}
	if cred == nil {
		return codexAppServerPeerCredentials{PID: pid, EUID: -1}, nil
	}
	return codexAppServerPeerCredentials{PID: pid, EUID: int(cred.Uid)}, nil
}
