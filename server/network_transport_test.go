package server

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
)

func TestTransportListenersUseConfiguredSelection(t *testing.T) {
	tests := []struct {
		name       string
		transports []string
		want       []string
		wantError  bool
	}{
		{name: "default", want: []string{"udp"}},
		{name: "raknet only", transports: []string{"raknet"}, want: []string{"udp"}},
		{name: "nethernet only", transports: []string{"nethernet"}, want: []string{"nethernet"}},
		{name: "both", transports: []string{"raknet", "nethernet"}, want: []string{"udp", "nethernet"}},
		{name: "empty name", transports: []string{""}, wantError: true},
		{name: "unknown name", transports: []string{"unknown"}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			user := DefaultConfig()
			user.Network.Address = "127.0.0.1:0"
			user.Network.Transport = test.transports
			user.Network.NetherNet.Address = "127.0.0.1:0"
			user.Network.NetherNet.KeyFile = filepath.Join(t.TempDir(), "identity.pem")
			user.Network.NetherNet.UDPPorts = "0"

			listeners, err := user.transportListeners()
			if test.wantError {
				if err == nil {
					t.Fatal("invalid transport selection was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal("valid transport selection was rejected")
			}
			if len(listeners) != len(test.want) {
				t.Fatalf("listener count = %d, want %d", len(listeners), len(test.want))
			}

			conf := Config{Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Allower: allower{}}
			for i, makeListener := range listeners {
				l, err := makeListener(conf)
				if err != nil {
					t.Fatal("selected listener could not start")
				}
				serverListener, ok := l.(listener)
				if !ok {
					_ = l.Close()
					t.Fatal("selected listener had an unexpected wrapper")
				}
				if got := serverListener.Listener.Addr().Network(); got != test.want[i] {
					_ = l.Close()
					t.Fatalf("listener %d network = %q, want %q", i, got, test.want[i])
				}
				if err := l.Close(); err != nil {
					t.Fatal("selected listener did not close")
				}
			}
		})
	}
}
