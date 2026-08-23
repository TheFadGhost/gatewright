package main

// demo-upstream serves a small synthetic upstream used by the README demo,
// local development and end-to-end tests. All data is synthetic.

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func demoUpstreamCmd(args []string) {
	fs := flagSet("demo-upstream")
	addr := fs.String("a", "127.0.0.1:9001", "listen address")
	fs.Parse(args)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/users/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/users/")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      id,
			"name":    "Synthetic User " + id,
			"plan":    "demo",
			"created": "2026-01-01T00:00:00Z",
		})
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"method":  r.Method,
			"path":    r.URL.Path,
			"query":   r.URL.RawQuery,
			"bytes":   len(body),
			"xff":     r.Header.Get("X-Forwarded-For"),
			"req_id":  r.Header.Get("X-Gatewright-Request-Id"),
		})
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		if ms <= 0 || ms > 60000 {
			ms = 1000
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("slow response after " + strconv.Itoa(ms) + "ms"))
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		kb, _ := strconv.Atoi(r.URL.Query().Get("kb"))
		if kb <= 0 || kb > 4*1024*1024 {
			kb = 1024
		}
		chunk := make([]byte, 32*1024)
		for i := range chunk {
			chunk[i] = 'x'
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		var written int
		for written < kb*1024 {
			n, err := w.Write(chunk)
			if err != nil {
				return
			}
			written += n
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})
	mux.HandleFunc("/ws", echoWebsocket)

	fmt.Printf("demo upstream listening on %s (endpoints: /healthz /users/{id} /echo /slow?ms= /big?kb= /ws)\n", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintln(osStderr(), "demo-upstream:", err)
		osExit(1)
	}
}

// echoWebsocket performs a minimal RFC6455 accept and echoes frames back.
func echoWebsocket(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if upgrade := strings.ToLower(r.Header.Get("Upgrade")); upgrade != "websocket" || key == "" {
		http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	h := sha1.New()
	_, _ = h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		return
	}

	for {
		frame, op, err := readWSFrame(buf.Reader)
		if err != nil {
			return
		}
		if op == 8 { // close
			_ = writeWSFrame(conn, op, frame)
			return
		}
		if err := writeWSFrame(conn, op, frame); err != nil {
			return
		}
	}
}

func readWSFrame(r io.Reader) ([]byte, byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, err
	}
	op := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	plen := uint64(header[1] & 0x7F)
	switch plen {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, 0, err
		}
		plen = uint64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, 0, err
		}
		plen = binary.BigEndian.Uint64(ext)
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return nil, 0, err
		}
	}
	payload := make([]byte, plen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return payload, op, nil
}

func writeWSFrame(w io.Writer, op byte, payload []byte) error {
	out := make([]byte, 0, len(payload)+10)
	out = append(out, 0x80|op)
	l := len(payload)
	switch {
	case l < 126:
		out = append(out, byte(l))
	case l < 65536:
		out = append(out, 126, byte(l>>8), byte(l))
	default:
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(l))
		out = append(out, 127)
		out = append(out, ext...)
	}
	out = append(out, payload...)
	_, err := w.Write(out)
	return err
}
