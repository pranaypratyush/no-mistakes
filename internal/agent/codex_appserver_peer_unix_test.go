//go:build linux || darwin

package agent

import (
	"strings"
	"testing"
)

func TestValidateCodexAppServerPeerCredentials(t *testing.T) {
	tests := []struct {
		name         string
		credentials  codexAppServerPeerCredentials
		effectiveUID int
		wantPID      int
		wantError    string
	}{
		{name: "matching effective user", credentials: codexAppServerPeerCredentials{PID: 42, EUID: 1000}, effectiveUID: 1000, wantPID: 42},
		{name: "missing pid", credentials: codexAppServerPeerCredentials{EUID: 1000}, effectiveUID: 1000, wantError: "no process id"},
		{name: "unavailable current euid", credentials: codexAppServerPeerCredentials{PID: 42, EUID: 1000}, effectiveUID: -1, wantError: "unavailable"},
		{name: "foreign effective user", credentials: codexAppServerPeerCredentials{PID: 42, EUID: 2000}, effectiveUID: 1000, wantError: "does not match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateCodexAppServerPeerCredentials(test.credentials, test.effectiveUID)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("validateCodexAppServerPeerCredentials() error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil || got != test.wantPID {
				t.Fatalf("validateCodexAppServerPeerCredentials() = (%d, %v), want (%d, nil)", got, err, test.wantPID)
			}
		})
	}
}
