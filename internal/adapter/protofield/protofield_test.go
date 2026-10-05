package protofield

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
)

// buildMsg encodes a tiny message: field 2 = string, field 5 = varint.
func buildMsg(s string, v int64) []byte {
	var b []byte
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, s)
	b = protowire.AppendTag(b, 5, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(v))
	return b
}

func TestExtractor_StringAndInt64(t *testing.T) {
	ext := New()
	msg := buildMsg("recipient-7", 3)

	got, err := ext.String(msg, 2)
	if err != nil || got != "recipient-7" {
		t.Fatalf("String(2) = %q, %v; want recipient-7, nil", got, err)
	}
	n, err := ext.Int64(msg, 5)
	if err != nil || n != 3 {
		t.Fatalf("Int64(5) = %d, %v; want 3, nil", n, err)
	}
}

func TestExtractor_AbsentFieldIsDomainSentinel(t *testing.T) {
	ext := New()
	msg := buildMsg("x", 1)
	if _, err := ext.String(msg, 9); !errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("String(absent) err = %v; want ErrFieldNotFound", err)
	}
	if _, err := ext.Int64(msg, 9); !errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("Int64(absent) err = %v; want ErrFieldNotFound", err)
	}
	// A string field requested as varint (wrong wire type) is also "not found".
	if _, err := ext.Int64(msg, 2); !errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("Int64(string-field) err = %v; want ErrFieldNotFound", err)
	}
	// And the inverse: a varint field requested as string.
	if _, err := ext.String(msg, 5); !errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("String(varint-field) err = %v; want ErrFieldNotFound", err)
	}
}

func TestExtractor_EmptyPayloadIsNotFound(t *testing.T) {
	ext := New()
	if _, err := ext.String(nil, 2); !errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("String(nil) err = %v; want ErrFieldNotFound", err)
	}
	if _, err := ext.Int64(nil, 5); !errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("Int64(nil) err = %v; want ErrFieldNotFound", err)
	}
}

// Malformed wire bytes are HARD errors (the caller nacks → DLQ) — never the
// "not found" sentinel and never a panic.
func TestExtractor_MalformedWire_HardErrors(t *testing.T) {
	ext := New()

	// 0x80 is a truncated varint tag (continuation bit, no next byte).
	badTag := []byte{0x80}

	// Tag for field 2 BytesType, length 5, only 2 bytes of data.
	truncBytes := append(protowire.AppendTag(nil, 2, protowire.BytesType), 0x05, 'a', 'b')

	// Tag for field 5 VarintType, then a truncated varint value.
	truncVarint := append(protowire.AppendTag(nil, 5, protowire.VarintType), 0x80)

	// A NON-matching field (1) whose value is malformed — trips the skip arm.
	badSkip := append(protowire.AppendTag(nil, 1, protowire.BytesType), 0x05, 'a', 'b')

	tests := []struct {
		name    string
		payload []byte
	}{
		{"truncated tag", badTag},
		{"matched bytes field truncated", truncBytes},
		{"matched varint field truncated", truncVarint},
		{"skipped field malformed", badSkip},
	}
	for _, tt := range tests {
		t.Run("String/"+tt.name, func(t *testing.T) {
			_, err := ext.String(tt.payload, 9)
			if err == nil || errors.Is(err, realtime.ErrFieldNotFound) {
				t.Fatalf("String err = %v; want hard wire error", err)
			}
		})
		t.Run("Int64/"+tt.name, func(t *testing.T) {
			_, err := ext.Int64(tt.payload, 9)
			if err == nil || errors.Is(err, realtime.ErrFieldNotFound) {
				t.Fatalf("Int64 err = %v; want hard wire error", err)
			}
		})
	}

	// The MATCHED-field truncation arms (field number == want) are distinct
	// from the skip arms above (want=9 skips everything).
	if _, err := ext.String(truncBytes, 2); err == nil || errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("String matched-truncated err = %v; want hard wire error", err)
	}
	if _, err := ext.Int64(truncVarint, 5); err == nil || errors.Is(err, realtime.ErrFieldNotFound) {
		t.Fatalf("Int64 matched-truncated err = %v; want hard wire error", err)
	}
}

func TestExtractor_FirstMatchWins(t *testing.T) {
	ext := New()
	var b []byte
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, "first")
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, "second")
	got, err := ext.String(b, 2)
	if err != nil || got != "first" {
		t.Fatalf("String = %q, %v; want first, nil", got, err)
	}
}
