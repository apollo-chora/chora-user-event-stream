// main_test.go — boot-path coverage for the connection-tier composition root.
//
// main() is a monolithic boot script (per the repo's composition-root
// convention) that ends by parking on <-ctx.Done() (SIGTERM), so the tests
// here run it IN-PROCESS on several representative env configurations and let
// the framework's process exit reclaim the (intentionally) leaked goroutine.
// A panic or log.Fatalf during any bootstrap would abort the whole test
// binary — which is exactly the fail-loud failure a boot smoke test exists to
// catch. There is deliberately no signal delivery: delivering SIGINT/SIGTERM
// programmatically is not portable on Windows, and the drain path after
// <-ctx.Done() is exercised only by live pods.
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// pinBootEnv pins every env var main()'s boot path consults so an ambient
// developer/CI environment can never leak into these tests.
func pinBootEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for _, k := range []string{
		"PORT",
		"CHORA_REALTIME_KEEPALIVE_SECONDS",
		"NATS_URL",
		"CHORA_SOURCE_PROJECT",
		"CHORA_REDIS_ADDR",
		"CHORA_REDIS_PASSWORD",
		"CHORA_REDIS_CA_CERT",
		"CHORA_REALTIME_TICKET_SIGNER_SECRET",
		"CHORA_REDIS_PASSWORD_SECRET_ID",
		"CHORA_REDIS_CA_CERT_SECRET_ID",
		"CHORA_REALTIME_TICKET_SIGNER_SECRET_ID",
	} {
		t.Setenv(k, "")
	}
	for k, v := range overrides {
		t.Setenv(k, v)
	}
}

// bootMain pins the env and runs main() in a goroutine, letting the boot path
// settle (either a fixed window or the extra handoff of an optional waiter
// sync — see the redis-wired subtest). PORT is left free via ":0" so the
// health/SSE listener can never collide with another test.
func bootMain(t *testing.T, overrides map[string]string, wait func(*testing.T)) {
	t.Helper()
	pinBootEnv(t, overrides)
	// PORT=0 binds an ephemeral port — the composition-root tests would
	// otherwise collide on the default :8080 and log.Fatalf kills the binary.
	t.Setenv("PORT", "0")
	go main()
	if wait == nil {
		time.Sleep(700 * time.Millisecond)
		return
	}
	wait(t)
	time.Sleep(200 * time.Millisecond)
}

// waitUntil polls cond until true or the deadline expires.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// bootRedis is a minimal dependency-free RESP3 stand-in for the backplane,
// used only to let the composition root's Store wiring succeed: it answers
// the go-redis dial handshake (HELLO 3) + PING and records what it saw so a
// test can assert the boot actually reached the Redis block.
type bootRedis struct {
	ln   net.Listener
	addr string

	mu   sync.Mutex
	ping int
}

func newBootRedis(t *testing.T) *bootRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("boot redis listen: %v", err)
	}
	b := &bootRedis{ln: ln, addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed by test cleanup
			}
			go b.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return b
}

func (b *bootRedis) pingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ping
}

func (b *bootRedis) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		cmd, err := readRESPCommand(rd)
		if err != nil {
			return // EOF / malformed — client closed or is gone
		}
		switch strings.ToUpper(cmd[0]) {
		case "HELLO":
			_, _ = c.Write([]byte("%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.0\r\n"))
		case "PING":
			b.mu.Lock()
			b.ping++
			b.mu.Unlock()
			_, _ = c.Write([]byte("+PONG\r\n"))
		default: // e.g. CLIENT SETINFO / MAINT-NOTIFICATIONS
			_, _ = c.Write([]byte("+OK\r\n"))
		}
	}
}

// readRESPCommand parses one RESP array-of-bulk-strings request.
func readRESPCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" || line[0] != '*' {
		return nil, fmt.Errorf("expected request array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		l = strings.TrimRight(l, "\r\n")
		if l == "" || l[0] != '$' {
			return nil, fmt.Errorf("expected bulk arg, got %q", l)
		}
		size, err := strconv.Atoi(l[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		if _, err := rd.Discard(2); err != nil { // trailing \r\n
			return nil, err
		}
		args = append(args, string(buf))
	}
	return args, nil
}

// deadPingServer accepts connections and immediately closes them: the
// go-redis dial handshake fails fast, so Store.Ping errors deterministically.
func deadPingServer(t *testing.T) (addr string, accepted func() int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("dead ping listen: %v", err)
	}
	var mu sync.Mutex
	var n int
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}

const testSigner = "0123456789abcdef0123456789abcdef" // ≥ minSignerBytes

func TestMainBootPaths(t *testing.T) {
	t.Run("health-only-streaming-disabled", func(t *testing.T) {
		// Nothing configured: the boot WARNING arms for redis + ticket
		// validation, the nil store (health-only stream handler), and the
		// health/SSE server all run.
		bootMain(t, nil, nil)
	})

	t.Run("redis-wired-validator-on", func(t *testing.T) {
		// Backplane wired (verified via the fake's PING) + a ticket signer set:
		// HMAC validator constructed AND the user bus handed to the handler.
		r := newBootRedis(t)
		bootMain(t, map[string]string{
			"CHORA_REDIS_ADDR":                    r.addr,
			"CHORA_REALTIME_TICKET_SIGNER_SECRET": testSigner,
		}, func(t *testing.T) {
			waitUntil(t, "redis PING from the composition root", func() bool {
				return r.pingCount() > 0
			})
		})
	})

	t.Run("invalid-ca-newstore-fails-soft", func(t *testing.T) {
		// NewStore's TLS build fails (bad CA PEM) → the "redis client build
		// failed" WARNING arm; boot continues health-only.
		bootMain(t, map[string]string{
			"CHORA_REDIS_ADDR":    "127.0.0.1:1",
			"CHORA_REDIS_CA_CERT": "not-a-pem",
		}, nil)
	})

	t.Run("redis-ping-fails", func(t *testing.T) {
		// Backplane reachable at TCP level but the handshake dies → the
		// "redis ping failed" WARNING arm; boot continues health-only. The
		// settle is generous because go-redis retries the dial with backoff
		// before Surfacing the error to main's Ping call.
		addr, accepted := deadPingServer(t)
		bootMain(t, map[string]string{"CHORA_REDIS_ADDR": addr}, func(t *testing.T) {
			waitUntil(t, "a connection attempt against the dead-ping server", func() bool {
				return accepted() > 0
			})
			time.Sleep(3 * time.Second) // dial-retry budget (pool backoff)
		})
	})
}
