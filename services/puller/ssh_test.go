package main

import "testing"

func TestParseSSHLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    sshLocation
		wantErr bool
	}{
		{
			name: "scp_like",
			raw:  "dev@10.0.0.8:/var/log/app.log",
			want: sshLocation{User: "dev", Host: "10.0.0.8", Port: 22, Path: "/var/log/app.log"},
		},
		{
			name: "ssh_url_with_port",
			raw:  "ssh://ops@example.com:2222/home/ops/events.jsonl",
			want: sshLocation{User: "ops", Host: "example.com", Port: 2222, Path: "/home/ops/events.jsonl"},
		},
		{
			name:    "invalid",
			raw:     "not-a-valid-location",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSSHLocation(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseSSHLocation() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSSHLocation() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseSSHLocation() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	t.Parallel()

	got := shellQuote("/var/log/it's.log")
	want := "'/var/log/it'\"'\"'s.log'"
	if got != want {
		t.Fatalf("shellQuote() = %q, want %q", got, want)
	}
}
