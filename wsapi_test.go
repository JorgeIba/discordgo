package discordgo

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestOpenReturnsOnInvalidSessionDuringOpen(t *testing.T) {
	server := newGatewayOpenTestServer(t, []byte(`{"op":9,"d":false}`))
	defer server.Close()
	session, err := newGatewayOpenTestSession(server, "Bot test")
	if err != nil {
		t.Fatalf("error creating session: %v", err)
	}
	session.sessionID = "old-session"
	session.resumeGatewayURL = session.gateway
	atomic.StoreInt64(session.sequence, 42)

	err = openWithTimeout(t, session)
	if err == nil {
		t.Fatal("expected Open to return an invalid session error")
	}

	if session.wsConn != nil {
		t.Fatal("Open returned an error without clearing the websocket")
	}
	if session.sessionID != "" {
		t.Fatalf("sessionID = %q, want empty", session.sessionID)
	}
	if session.resumeGatewayURL != "" {
		t.Fatalf("resumeGatewayURL = %q, want empty", session.resumeGatewayURL)
	}
	if sequence := atomic.LoadInt64(session.sequence); sequence != 0 {
		t.Fatalf("sequence = %d, want 0", sequence)
	}
}

func TestOpenReturnsOnHeartbeatAckDuringOpen(t *testing.T) {
	server := newGatewayOpenTestServer(t, []byte(`{"op":11,"d":null}`))
	defer server.Close()
	session, err := newGatewayOpenTestSession(server, "Bot test")
	if err != nil {
		t.Fatalf("error creating session: %v", err)
	}

	err = openWithTimeout(t, session)
	if err == nil {
		t.Fatal("expected Open to return an unexpected operation error")
	}
	if session.wsConn != nil {
		t.Fatal("Open returned an error without clearing the websocket")
	}
}

// A successful resume replays buffered dispatches before the RESUMED marker, so
// a dispatch arriving as the first post-resume frame must NOT abort Open;
// otherwise resuming would churn reconnects.
func TestOpenAllowsDispatchDuringOpen(t *testing.T) {
	server := newGatewayOpenTestServer(t, []byte(`{"op":0,"s":1,"t":"PRESENCE_UPDATE","d":{"guild_id":"guild","user":{"id":"user"},"status":"online","activities":[],"client_status":{}}}`))
	defer server.Close()
	session, err := newGatewayOpenTestSession(server, "Bot test")
	if err != nil {
		t.Fatalf("error creating session: %v", err)
	}
	session.ShouldReconnectOnError = false
	session.sessionID = "old-session"
	session.resumeGatewayURL = session.gateway
	atomic.StoreInt64(session.sequence, 42)
	defer session.Close()

	if err = openWithTimeout(t, session); err != nil {
		t.Fatalf("expected Open to succeed on a replayed dispatch before RESUMED, got: %v", err)
	}
}

func newGatewayOpenTestServer(t *testing.T, startupPacket []byte) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"op":10,"d":{"heartbeat_interval":1000}}`)); err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, startupPacket); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}))

	return server
}

func newGatewayOpenTestSession(server *httptest.Server, token string) (*Session, error) {
	session, err := New(token)
	if err != nil {
		return nil, err
	}
	session.gateway = "ws" + strings.TrimPrefix(server.URL, "http")
	return session, nil
}

func openWithTimeout(t *testing.T, session *Session) error {
	t.Helper()

	var err error
	errCh := make(chan error, 1)
	go func() {
		errCh <- session.Open()
	}()

	select {
	case err = <-errCh:
		return err
	case <-time.After(time.Second):
		t.Fatal("Open did not return")
	}

	return err
}

// A heartbeat request may arrive after Identify or Resume and before the first
// dispatch. Open must reply without reacquiring its session lock and keep
// reading startup frames, including another heartbeat request.
func TestOpenAnswersHeartbeatRequestsDuringHandshake(t *testing.T) {
	for _, resume := range []bool{false, true} {
		for _, compressed := range []bool{false, true} {
			for _, nextOp := range []int{0, 7, 9, 11} {
				t.Run(fmt.Sprintf("resume=%t/compressed=%t/next_op=%d", resume, compressed, nextOp), func(t *testing.T) {
					result := make(chan error, 1)
					upgrader := websocket.Upgrader{}
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							result <- err
							return
						}
						defer conn.Close()
						_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
						if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"op":10,"d":{"heartbeat_interval":60000}}`)); err != nil {
							result <- err
							return
						}
						var auth struct{ Op int }
						if err = conn.ReadJSON(&auth); err != nil {
							result <- err
							return
						}
						wantAuth := 2
						if resume {
							wantAuth = 6
						}
						if auth.Op != wantAuth {
							result <- fmt.Errorf("auth opcode = %d, want %d", auth.Op, wantAuth)
							return
						}
						wantSequence := int64(0)
						if resume {
							wantSequence = 42
						}
						for i := 0; i < 2; i++ {
							if err = writeHandshakeTestFrame(conn, []byte(`{"op":1,"d":null}`), compressed); err != nil {
								result <- err
								return
							}
							var heartbeat struct {
								Op   int
								Data int64 `json:"d"`
							}
							if err = conn.ReadJSON(&heartbeat); err != nil {
								result <- fmt.Errorf("heartbeat reply: %w", err)
								return
							}
							if heartbeat.Op != 1 || heartbeat.Data != wantSequence {
								result <- fmt.Errorf("heartbeat = %+v, want opcode 1 and sequence %d", heartbeat, wantSequence)
								return
							}
							if err = writeHandshakeTestFrame(conn, []byte(`{"op":11,"d":null}`), compressed); err != nil {
								result <- err
								return
							}
						}
						packet := []byte(fmt.Sprintf(`{"op":%d,"d":false}`, nextOp))
						if nextOp == 0 {
							packet = []byte(`{"op":0,"s":43,"t":"READY","d":{"v":9,"session_id":"fresh-session","user":{"id":"123","username":"test"},"guilds":[]}}`)
							if resume {
								packet = []byte(`{"op":0,"s":43,"t":"RESUMED","d":{}}`)
							}
						}
						if err = writeHandshakeTestFrame(conn, packet, compressed); err != nil {
							result <- err
							return
						}
						result <- nil
						// Keep the connection alive until Open has completed and the test closes it.
						for {
							if _, _, err = conn.ReadMessage(); err != nil {
								return
							}
						}
					}))
					defer server.Close()
					session, err := newGatewayOpenTestSession(server, "Bot test")
					if err != nil {
						t.Fatal(err)
					}
					session.ShouldReconnectOnError = false
					if resume {
						session.sessionID = "old-session"
						session.resumeGatewayURL = session.gateway
						atomic.StoreInt64(session.sequence, 42)
					}
					err = openWithTimeout(t, session)
					if nextOp == 0 {
						if err != nil {
							t.Fatalf("Open rejected heartbeat request: %v", err)
						}
						defer session.Close()
						if atomic.LoadInt64(session.sequence) != 43 {
							t.Fatal("Open returned before processing the startup dispatch")
						}
						if !resume && session.sessionID != "fresh-session" {
							t.Fatal("READY was not processed")
						}
					} else {
						if err == nil {
							defer session.Close()
							t.Fatal("expected control event to abort Open")
						}
						if session.wsConn != nil {
							t.Fatal("failed Open retained its websocket")
						}
						if nextOp == 9 && session.sessionID != "" {
							t.Fatal("invalid session was not cleared")
						}
					}
					select {
					case err := <-result:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("gateway did not finish heartbeat handshake")
					}
				})
			}
		}
	}
}

func writeHandshakeTestFrame(conn *websocket.Conn, packet []byte, compressed bool) error {
	if !compressed {
		return conn.WriteMessage(websocket.TextMessage, packet)
	}
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	if _, err := writer.Write(packet); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}
