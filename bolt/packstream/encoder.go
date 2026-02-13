// Package packstream implements Neo4j's PackStream binary serialization format.
// Adapted from github.com/johnnadratowski/golang-neo4j-bolt-driver (MIT License)
package packstream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
)

// Marker bytes for PackStream encoding
const (
	NilMarker   = 0xC0
	TrueMarker  = 0xC3
	FalseMarker = 0xC2

	Int8Marker  = 0xC8
	Int16Marker = 0xC9
	Int32Marker = 0xCA
	Int64Marker = 0xCB

	FloatMarker = 0xC1

	TinyStringMarker = 0x80
	String8Marker    = 0xD0
	String16Marker   = 0xD1
	String32Marker   = 0xD2

	TinySliceMarker = 0x90
	Slice8Marker    = 0xD4
	Slice16Marker   = 0xD5
	Slice32Marker   = 0xD6

	TinyMapMarker = 0xA0
	Map8Marker    = 0xD8
	Map16Marker   = 0xD9
	Map32Marker   = 0xDA

	TinyStructMarker = 0xB0
	Struct8Marker    = 0xDC
	Struct16Marker   = 0xDD
)

// EndMessage marks the end of a message
var EndMessage = []byte{0x00, 0x00}

// Structure is an interface for PackStream structures
type Structure interface {
	Signature() byte
	Fields() []interface{}
}

// Encoder encodes values to PackStream format
type Encoder struct {
	w         io.Writer
	buf       *bytes.Buffer
	chunkSize uint16
}

// NewEncoder creates a new encoder
func NewEncoder(w io.Writer, chunkSize uint16) *Encoder {
	return &Encoder{
		w:         w,
		buf:       &bytes.Buffer{},
		chunkSize: chunkSize,
	}
}

// Marshal encodes a value to PackStream bytes
func Marshal(v interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := NewEncoder(buf, math.MaxUint16)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Write writes to the internal buffer
func (e *Encoder) Write(p []byte) (int, error) {
	return e.buf.Write(p)
}

// Flush writes buffered data as chunks to the underlying writer
func (e *Encoder) Flush() error {
	length := e.buf.Len()
	if length > 0 {
		if err := binary.Write(e.w, binary.BigEndian, uint16(length)); err != nil {
			return fmt.Errorf("failed to write length: %w", err)
		}
		if _, err := e.buf.WriteTo(e.w); err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		}
	}

	if _, err := e.w.Write(EndMessage); err != nil {
		return fmt.Errorf("failed to write end message: %w", err)
	}
	e.buf.Reset()
	return nil
}

// Encode encodes a value and flushes
func (e *Encoder) Encode(v interface{}) error {
	if err := e.encode(v); err != nil {
		return err
	}
	return e.Flush()
}

// EncodeValue encodes a value without flushing
func (e *Encoder) EncodeValue(v interface{}) error {
	return e.encode(v)
}

func (e *Encoder) encode(v interface{}) error {
	switch val := v.(type) {
	case nil:
		return e.encodeNil()
	case bool:
		return e.encodeBool(val)
	case int:
		return e.encodeInt(int64(val))
	case int8:
		return e.encodeInt(int64(val))
	case int16:
		return e.encodeInt(int64(val))
	case int32:
		return e.encodeInt(int64(val))
	case int64:
		return e.encodeInt(val)
	case uint:
		return e.encodeInt(int64(val))
	case uint8:
		return e.encodeInt(int64(val))
	case uint16:
		return e.encodeInt(int64(val))
	case uint32:
		return e.encodeInt(int64(val))
	case uint64:
		if val > math.MaxInt64 {
			return fmt.Errorf("integer too large: %d", val)
		}
		return e.encodeInt(int64(val))
	case float32:
		return e.encodeFloat(float64(val))
	case float64:
		return e.encodeFloat(val)
	case string:
		return e.encodeString(val)
	case []interface{}:
		return e.encodeSlice(val)
	case map[string]interface{}:
		return e.encodeMap(val)
	case Structure:
		return e.encodeStructure(val)
	default:
		// Handle arbitrary slice types
		if reflect.TypeOf(v).Kind() == reflect.Slice {
			s := reflect.ValueOf(v)
			slice := make([]interface{}, s.Len())
			for i := 0; i < s.Len(); i++ {
				slice[i] = s.Index(i).Interface()
			}
			return e.encodeSlice(slice)
		}
		return fmt.Errorf("unsupported type: %T", v)
	}
}

func (e *Encoder) encodeNil() error {
	_, err := e.Write([]byte{NilMarker})
	return err
}

func (e *Encoder) encodeBool(val bool) error {
	if val {
		_, err := e.Write([]byte{TrueMarker})
		return err
	}
	_, err := e.Write([]byte{FalseMarker})
	return err
}

func (e *Encoder) encodeInt(val int64) error {
	switch {
	case val >= -16 && val <= 127:
		return binary.Write(e, binary.BigEndian, int8(val))
	case val >= math.MinInt8 && val < -16:
		if _, err := e.Write([]byte{Int8Marker}); err != nil {
			return err
		}
		return binary.Write(e, binary.BigEndian, int8(val))
	case val > 127 && val <= math.MaxInt16:
		if _, err := e.Write([]byte{Int16Marker}); err != nil {
			return err
		}
		return binary.Write(e, binary.BigEndian, int16(val))
	case val >= math.MinInt16 && val < math.MinInt8:
		if _, err := e.Write([]byte{Int16Marker}); err != nil {
			return err
		}
		return binary.Write(e, binary.BigEndian, int16(val))
	case val > math.MaxInt16 && val <= math.MaxInt32:
		if _, err := e.Write([]byte{Int32Marker}); err != nil {
			return err
		}
		return binary.Write(e, binary.BigEndian, int32(val))
	case val >= math.MinInt32 && val < math.MinInt16:
		if _, err := e.Write([]byte{Int32Marker}); err != nil {
			return err
		}
		return binary.Write(e, binary.BigEndian, int32(val))
	default:
		if _, err := e.Write([]byte{Int64Marker}); err != nil {
			return err
		}
		return binary.Write(e, binary.BigEndian, val)
	}
}

func (e *Encoder) encodeFloat(val float64) error {
	if _, err := e.Write([]byte{FloatMarker}); err != nil {
		return err
	}
	return binary.Write(e, binary.BigEndian, val)
}

func (e *Encoder) encodeString(val string) error {
	b := []byte(val)
	length := len(b)

	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyStringMarker + length)}); err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if _, err := e.Write([]byte{String8Marker, byte(length)}); err != nil {
			return err
		}
	case length <= math.MaxUint16:
		if _, err := e.Write([]byte{String16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint16(length)); err != nil {
			return err
		}
	default:
		if _, err := e.Write([]byte{String32Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint32(length)); err != nil {
			return err
		}
	}

	_, err := e.Write(b)
	return err
}

func (e *Encoder) encodeSlice(val []interface{}) error {
	length := len(val)

	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinySliceMarker + length)}); err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if _, err := e.Write([]byte{Slice8Marker, byte(length)}); err != nil {
			return err
		}
	case length <= math.MaxUint16:
		if _, err := e.Write([]byte{Slice16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint16(length)); err != nil {
			return err
		}
	default:
		if _, err := e.Write([]byte{Slice32Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint32(length)); err != nil {
			return err
		}
	}

	for _, item := range val {
		if err := e.encode(item); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeMap(val map[string]interface{}) error {
	length := len(val)

	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyMapMarker + length)}); err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if _, err := e.Write([]byte{Map8Marker, byte(length)}); err != nil {
			return err
		}
	case length <= math.MaxUint16:
		if _, err := e.Write([]byte{Map16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint16(length)); err != nil {
			return err
		}
	default:
		if _, err := e.Write([]byte{Map32Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint32(length)); err != nil {
			return err
		}
	}

	for k, v := range val {
		if err := e.encode(k); err != nil {
			return err
		}
		if err := e.encode(v); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeStructure(val Structure) error {
	fields := val.Fields()
	length := len(fields)

	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyStructMarker + length)}); err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if _, err := e.Write([]byte{Struct8Marker, byte(length)}); err != nil {
			return err
		}
	default:
		if _, err := e.Write([]byte{Struct16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, uint16(length)); err != nil {
			return err
		}
	}

	if _, err := e.Write([]byte{val.Signature()}); err != nil {
		return err
	}

	for _, field := range fields {
		if err := e.encode(field); err != nil {
			return err
		}
	}
	return nil
}
