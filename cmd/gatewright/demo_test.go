package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// wsClientFrame builds a client-to-server frame (masked) around a payload.
func wsClientFrame(t *testing.T, op byte, payload []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	out.WriteByte(0x80 | op)
	l := len(payload)
	switch {
	case l < 126:
		out.WriteByte(byte(l) | 0x80)
	case l < 65536:
		out.WriteByte(126 | 0x80)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(l))
		out.Write(ext[:])
	default:
		out.WriteByte(127 | 0x80)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(l))
		out.Write(ext[:])
	}
	var mask = [4]byte{0x11, 0x22, 0x33, 0x44}
	out.Write(mask[:])
	masked := make([]byte, l)
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	out.Write(masked)
	return out.Bytes()
}

func TestReadWSFrameEchoesUpToCap(t *testing.T) {
	for _, size := range []int{0, 1, 125, 126, 4096, maxWSFramePayload - 1, maxWSFramePayload} {
		payload := []byte(strings.Repeat("a", size))
		frame, op, err := readWSFrame(bytes.NewReader(wsClientFrame(t, 1, payload)))
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if op != 1 {
			t.Errorf("size %d: opcode = %d", size, op)
		}
		if !bytes.Equal(frame, payload) {
			t.Errorf("size %d: payload mismatch (%d bytes back)", size, len(frame))
		}
	}
}

func TestReadWSFrameRejectsOversizedDeclaredLength(t *testing.T) {
	const declared = int64(maxWSFramePayload) + 1 // attacker-declared length
	var out bytes.Buffer
	out.WriteByte(0x80 | 1) // FIN + text, unmasked for brevity of the header only
	out.WriteByte(127)      // 64-bit length follows
	var ext [8]byte
	binary.BigEndian.PutUint64(ext[:], uint64(declared))
	out.Write(ext[:])
	out.Write(bytes.Repeat([]byte{'b'}, maxWSFramePayload+1)) // actually stream past the cap

	_, _, err := readWSFrame(&out)
	if !errors.Is(err, errWSFrameTooLarge) {
		t.Fatalf("err = %v, want errWSFrameTooLarge", err)
	}
}

func TestReadWSFrameRejectsOversizeWithoutHugeBuffer(t *testing.T) {
	// A peer declares a gigantic 64-bit length but the reader must never
	// preallocate it: streaming stops after cap+1 bytes and errors out.
	var out bytes.Buffer
	out.WriteByte(0x80 | 2)
	out.WriteByte(127)
	var ext [8]byte
	binary.BigEndian.PutUint64(ext[:], 1<<40) // 1 TiB declared
	out.Write(ext[:])
	out.Write(bytes.Repeat([]byte{'c'}, 1024)) // far less than declared

	_, _, err := readWSFrame(&out)
	if err == nil {
		t.Fatal("expected an error (too large or unexpected EOF), got nil")
	}
	if err != io.ErrUnexpectedEOF && !errors.Is(err, errWSFrameTooLarge) {
		t.Fatalf("err = %v, want errWSFrameTooLarge or ErrUnexpectedEOF", err)
	}
}

func TestReadWSFrameShortPayloadIsUnexpectedEOF(t *testing.T) {
	var out bytes.Buffer
	out.WriteByte(0x80 | 1)
	out.WriteByte(10) // declare 10 bytes...
	out.WriteString("short") // ...send 5
	if _, _, err := readWSFrame(&out); err != io.ErrUnexpectedEOF {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}
