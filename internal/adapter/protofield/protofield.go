// Package protofield is a minimal, read-only protobuf wire-format field
// extractor — the symmetric inverse of the producer-side hand-rolled protowire
// encoders in chora-delivery/chora-sharing, mirroring
// chora-notifications/internal/adapter/events/protofield but adding an Int64
// (varint) reader for the Companion level-up stage fields.
//
// Why hand-rolled: regenerating chora-contracts/gen/go fan-trips ~28 service
// builds (feedback_contracts_gen_go_fan_trips_all_triggers); chora-realtime
// reads exactly two field kinds (a recipient string + a stage varint) from
// otherwise-opaque binary events, so it needs no generated bindings.
//
// Scope: top-level fields only. It implements realtime.FieldExtractor and
// returns realtime.ErrFieldNotFound (the domain sentinel) on absence so the
// domain stays self-contained.
package protofield

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
)

// Extractor implements realtime.FieldExtractor over binary proto payloads.
type Extractor struct{}

// New returns the stateless extractor.
func New() Extractor { return Extractor{} }

// String returns the FIRST top-level length-delimited (string/bytes) field with
// the given number, decoded UTF-8. Returns realtime.ErrFieldNotFound when the
// field is absent (or present only under a non-bytes wire type). A malformed
// wire byte is a hard error the caller MUST surface (nack → DLQ).
func (Extractor) String(payload []byte, field int) (string, error) {
	want := protowire.Number(field)
	b := payload
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", fmt.Errorf("protofield: bad tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if num == want && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return "", fmt.Errorf("protofield: bad bytes field %d: %w", field, protowire.ParseError(m))
			}
			return string(v), nil
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return "", fmt.Errorf("protofield: bad field %d value: %w", num, protowire.ParseError(m))
		}
		b = b[m:]
	}
	return "", realtime.ErrFieldNotFound
}

// Int64 returns the FIRST top-level varint field (proto int32/int64/enum/bool
// all encode as varint) with the given number. Returns realtime.ErrFieldNotFound
// when absent — proto3 omits zero-value scalars, so callers treat absence as 0.
func (Extractor) Int64(payload []byte, field int) (int64, error) {
	want := protowire.Number(field)
	b := payload
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, fmt.Errorf("protofield: bad tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if num == want && typ == protowire.VarintType {
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return 0, fmt.Errorf("protofield: bad varint field %d: %w", field, protowire.ParseError(m))
			}
			return int64(v), nil
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return 0, fmt.Errorf("protofield: bad field %d value: %w", num, protowire.ParseError(m))
		}
		b = b[m:]
	}
	return 0, realtime.ErrFieldNotFound
}

// Compile-time assertion: Extractor satisfies the domain port.
var _ realtime.FieldExtractor = Extractor{}
