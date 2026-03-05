package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDispatchToChannelSMTPNoneMode(t *testing.T) {
	t.Parallel()

	smtpServer := newFakeSMTPServer(t)
	defer smtpServer.Close()

	dispatcher := &alertDispatcher{
		cfg: integrationConfig{
			WebhookTimeout:       2 * time.Second,
			EmailSMTPHost:        smtpServer.Host(),
			EmailSMTPPort:        smtpServer.Port(),
			EmailFrom:            "alerts@example.com",
			EmailSMTPTLSMode:     smtpTLSModeNone,
			EmailSMTPUser:        "",
			EmailSMTPPass:        "",
			CallbackSignatureTTL: 0,
		},
	}

	payload := []byte(`{"severity":"warning","email_to":["ops@example.com","sre@example.com"]}`)
	if err := dispatcher.dispatchToChannel("", channelEmail, payload, eventTypeAlert); err != nil {
		t.Fatalf("dispatchToChannel returned error: %v", err)
	}

	message := smtpServer.WaitForMessage(t)

	if message.mailFrom != "<alerts@example.com>" {
		t.Fatalf("mail from mismatch: got %q want %q", message.mailFrom, "<alerts@example.com>")
	}

	gotRecipients := append([]string(nil), message.rcptTo...)
	sort.Strings(gotRecipients)
	wantRecipients := []string{"<ops@example.com>", "<sre@example.com>"}
	sort.Strings(wantRecipients)
	if strings.Join(gotRecipients, ",") != strings.Join(wantRecipients, ",") {
		t.Fatalf("rcpt to mismatch: got %v want %v", gotRecipients, wantRecipients)
	}

	if !strings.Contains(message.data, "Subject: [agentledger][alert][warning]") {
		t.Fatalf("smtp message subject mismatch: %s", message.data)
	}
	if !strings.Contains(message.data, "[agentledger][alert]") {
		t.Fatalf("smtp message body mismatch: %s", message.data)
	}
}

func TestDispatchToChannelSMTPStartTLSUnsupported(t *testing.T) {
	t.Parallel()

	smtpServer := newFakeSMTPServer(t)
	defer smtpServer.Close()

	dispatcher := &alertDispatcher{
		cfg: integrationConfig{
			WebhookTimeout:       2 * time.Second,
			EmailSMTPHost:        smtpServer.Host(),
			EmailSMTPPort:        smtpServer.Port(),
			EmailFrom:            "alerts@example.com",
			EmailSMTPTLSMode:     smtpTLSModeSTARTTLS,
			EmailSMTPUser:        "",
			EmailSMTPPass:        "",
			CallbackSignatureTTL: 0,
		},
	}

	err := dispatcher.dispatchToChannel("", channelEmail, []byte(`{"severity":"critical","email_to":["ops@example.com"]}`), eventTypeAlert)
	if err == nil {
		t.Fatal("expected starttls unsupported error")
	}
	if !strings.Contains(err.Error(), "STARTTLS") {
		t.Fatalf("error mismatch: %v", err)
	}
	if isRetryable(err) {
		t.Fatalf("starttls unsupported should be non-retryable: %v", err)
	}
}

type fakeSMTPServer struct {
	listener net.Listener
	host     string
	port     int

	messages chan fakeSMTPMessage
	stop     chan struct{}
	wg       sync.WaitGroup
}

type fakeSMTPMessage struct {
	mailFrom string
	rcptTo   []string
	data     string
}

func newFakeSMTPServer(t *testing.T) *fakeSMTPServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake smtp server failed: %v", err)
	}

	host, portRaw, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatalf("split host port failed: %v", err)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		listener.Close()
		t.Fatalf("parse port failed: %v", err)
	}

	server := &fakeSMTPServer{
		listener: listener,
		host:     host,
		port:     port,
		messages: make(chan fakeSMTPMessage, 8),
		stop:     make(chan struct{}),
	}

	server.wg.Add(1)
	go server.acceptLoop()

	return server
}

func (s *fakeSMTPServer) Host() string {
	return s.host
}

func (s *fakeSMTPServer) Port() int {
	return s.port
}

func (s *fakeSMTPServer) Close() {
	close(s.stop)
	_ = s.listener.Close()
	s.wg.Wait()
}

func (s *fakeSMTPServer) WaitForMessage(t *testing.T) fakeSMTPMessage {
	t.Helper()

	select {
	case message := <-s.messages:
		return message
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting smtp message")
		return fakeSMTPMessage{}
	}
}

func (s *fakeSMTPServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
			}
			return
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *fakeSMTPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writeLine := func(line string) error {
		if _, err := io.WriteString(writer, line+"\r\n"); err != nil {
			return err
		}
		return writer.Flush()
	}

	if err := writeLine("220 fake-smtp ESMTP ready"); err != nil {
		return
	}

	message := fakeSMTPMessage{}
	var data bytes.Buffer

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(line)

		switch {
		case strings.HasPrefix(upper, "EHLO "), strings.HasPrefix(upper, "HELO "):
			if err := writeLine("250-fake-smtp"); err != nil {
				return
			}
			if err := writeLine("250 OK"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "MAIL FROM:"):
			message.mailFrom = strings.TrimSpace(line[len("MAIL FROM:"):])
			if err := writeLine("250 2.1.0 OK"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "RCPT TO:"):
			message.rcptTo = append(message.rcptTo, strings.TrimSpace(line[len("RCPT TO:"):]))
			if err := writeLine("250 2.1.5 OK"); err != nil {
				return
			}
		case upper == "DATA":
			if err := writeLine("354 End data with <CR><LF>.<CR><LF>"); err != nil {
				return
			}
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" || dataLine == ".\n" {
					break
				}
				_, _ = data.WriteString(dataLine)
			}
			message.data = data.String()
			if err := writeLine("250 2.0.0 queued"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "AUTH "):
			if err := writeLine("235 2.7.0 Auth OK"); err != nil {
				return
			}
		case upper == "QUIT":
			if err := writeLine("221 2.0.0 Bye"); err != nil {
				return
			}
			select {
			case s.messages <- message:
			default:
			}
			return
		case upper == "RSET", upper == "NOOP":
			if err := writeLine("250 OK"); err != nil {
				return
			}
		default:
			if strings.HasPrefix(upper, "STARTTLS") {
				_ = writeLine("454 4.7.0 TLS not available")
				return
			}
			_ = writeLine("250 OK")
		}
	}
}

func TestStringListUnmarshalJSON(t *testing.T) {
	t.Parallel()

	var list stringList
	if err := list.UnmarshalJSON([]byte(`"a@example.com, b@example.com"`)); err != nil {
		t.Fatalf("unmarshal string failed: %v", err)
	}
	if got := strings.Join(list, ","); got != "a@example.com,b@example.com" {
		t.Fatalf("string list mismatch: got %q", got)
	}

	if err := list.UnmarshalJSON([]byte(`["a@example.com","b@example.com"]`)); err != nil {
		t.Fatalf("unmarshal array failed: %v", err)
	}
	if got := strings.Join(list, ","); got != "a@example.com,b@example.com" {
		t.Fatalf("array list mismatch: got %q", got)
	}

	if err := list.UnmarshalJSON([]byte(`1`)); err == nil {
		t.Fatal("expected invalid list json to fail")
	}
}

func TestResolveEmailRecipientsFallback(t *testing.T) {
	t.Parallel()

	got, err := resolveEmailRecipients([]byte(`{"severity":"warning"}`), "alerts@example.com")
	if err != nil {
		t.Fatalf("resolveEmailRecipients returned error: %v", err)
	}
	if fmt.Sprintf("%v", got) != "[alerts@example.com]" {
		t.Fatalf("fallback recipients mismatch: got %v", got)
	}
}

func TestResolveEmailRecipientsInvalidAddress(t *testing.T) {
	t.Parallel()

	_, err := resolveEmailRecipients([]byte(`{"email_to":["invalid address"]}`), "")
	if err == nil {
		t.Fatal("expected invalid recipient to fail")
	}
	if !strings.Contains(err.Error(), "invalid recipient") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestBuildEmailWebhookPayloadUsesPayloadOccurredAt(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{
		cfg: integrationConfig{
			EmailFrom: "alerts@example.com",
		},
	}

	rawOccurredAt := "2026-03-05T03:04:05.000Z"
	payload := []byte(fmt.Sprintf(`{"severity":"warning","email_to":["ops@example.com"],"occurred_at":"%s"}`, rawOccurredAt))

	data, err := dispatcher.buildEmailWebhookPayload("", payload, eventTypeAlert)
	if err != nil {
		t.Fatalf("buildEmailWebhookPayload returned error: %v", err)
	}

	var wrapped emailWebhookChannelPayload
	if err := json.Unmarshal(data, &wrapped); err != nil {
		t.Fatalf("unmarshal email webhook payload failed: %v", err)
	}

	wantOccurredAt, err := time.Parse(time.RFC3339Nano, rawOccurredAt)
	if err != nil {
		t.Fatalf("parse occurred_at failed: %v", err)
	}
	if !wrapped.OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("occurred_at mismatch: got %s want %s", wrapped.OccurredAt.UTC().Format(time.RFC3339Nano), wantOccurredAt.UTC().Format(time.RFC3339Nano))
	}
}

func TestBuildEmailWebhookPayloadKeepsOccurredAtStableByMessageKey(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{
		cfg: integrationConfig{
			Channels:    []integrationChannel{channelEmailWebhook},
			EmailFrom:   "alerts@example.com",
			ChannelURLs: map[integrationChannel]string{},
		},
		progress: newDispatchProgressStore(),
	}

	payload := []byte(`{"severity":"warning"}`)

	firstRaw, err := dispatcher.buildEmailWebhookPayload("GOVERNANCE_ALERTS/42", payload, eventTypeAlert)
	if err != nil {
		t.Fatalf("buildEmailWebhookPayload first call failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	secondRaw, err := dispatcher.buildEmailWebhookPayload("GOVERNANCE_ALERTS/42", payload, eventTypeAlert)
	if err != nil {
		t.Fatalf("buildEmailWebhookPayload second call failed: %v", err)
	}

	var firstPayload emailWebhookChannelPayload
	if err := json.Unmarshal(firstRaw, &firstPayload); err != nil {
		t.Fatalf("unmarshal first payload failed: %v", err)
	}
	var secondPayload emailWebhookChannelPayload
	if err := json.Unmarshal(secondRaw, &secondPayload); err != nil {
		t.Fatalf("unmarshal second payload failed: %v", err)
	}

	if firstPayload.OccurredAt.IsZero() {
		t.Fatal("first payload occurred_at should not be zero")
	}
	if !firstPayload.OccurredAt.Equal(secondPayload.OccurredAt) {
		t.Fatalf("occurred_at should be stable for same message key: first=%s second=%s", firstPayload.OccurredAt.UTC().Format(time.RFC3339Nano), secondPayload.OccurredAt.UTC().Format(time.RFC3339Nano))
	}
}
