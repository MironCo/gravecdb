package packstream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Decoder decodes PackStream format
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a new decoder
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Unmarshal decodes PackStream bytes to a value
func Unmarshal(b []byte) (interface{}, error) {
	return NewDecoder(bytes.NewReader(b)).Decode()
}

// ReadMessage reads a chunked message from the stream
func (d *Decoder) ReadMessage() (*bytes.Buffer, error) {
	output := &bytes.Buffer{}

	for {
		var length uint16
		if err := binary.Read(d.r, binary.BigEndian, &length); err != nil {
			return nil, fmt.Errorf("failed to read chunk length: %w", err)
		}

		if length == 0 {
			// End of message
			return output, nil
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(d.r, data); err != nil {
			return nil, fmt.Errorf("failed to read chunk data: %w", err)
		}

		output.Write(data)
	}
}

// Decode reads and decodes a message
func (d *Decoder) Decode() (interface{}, error) {
	data, err := d.ReadMessage()
	if err != nil {
		return nil, err
	}
	return d.decodeValue(data)
}

// DecodeValue decodes a value from a buffer
func (d *Decoder) decodeValue(buf *bytes.Buffer) (interface{}, error) {
	marker, err := buf.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read marker: %w", err)
	}

	// Check for tiny int (high nibble indicates type)
	markerInt := int8(marker)

	switch {
	// Nil
	case marker == NilMarker:
		return nil, nil

	// Bool
	case marker == TrueMarker:
		return true, nil
	case marker == FalseMarker:
		return false, nil

	// Int
	case markerInt >= -16 && markerInt <= 127:
		return int64(markerInt), nil
	case marker == Int8Marker:
		var val int8
		if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return int64(val), nil
	case marker == Int16Marker:
		var val int16
		if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return int64(val), nil
	case marker == Int32Marker:
		var val int32
		if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return int64(val), nil
	case marker == Int64Marker:
		var val int64
		if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	// Float
	case marker == FloatMarker:
		var val float64
		if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	// String
	case marker >= TinyStringMarker && marker <= TinyStringMarker+0x0F:
		size := int(marker - TinyStringMarker)
		if size == 0 {
			return "", nil
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(buf, data); err != nil {
			return nil, err
		}
		return string(data), nil
	case marker == String8Marker:
		var size uint8
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(buf, data); err != nil {
			return nil, err
		}
		return string(data), nil
	case marker == String16Marker:
		var size uint16
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(buf, data); err != nil {
			return nil, err
		}
		return string(data), nil
	case marker == String32Marker:
		var size uint32
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(buf, data); err != nil {
			return nil, err
		}
		return string(data), nil

	// Slice
	case marker >= TinySliceMarker && marker <= TinySliceMarker+0x0F:
		size := int(marker - TinySliceMarker)
		return d.decodeSlice(buf, size)
	case marker == Slice8Marker:
		var size uint8
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeSlice(buf, int(size))
	case marker == Slice16Marker:
		var size uint16
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeSlice(buf, int(size))
	case marker == Slice32Marker:
		var size uint32
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeSlice(buf, int(size))

	// Map
	case marker >= TinyMapMarker && marker <= TinyMapMarker+0x0F:
		size := int(marker - TinyMapMarker)
		return d.decodeMap(buf, size)
	case marker == Map8Marker:
		var size uint8
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeMap(buf, int(size))
	case marker == Map16Marker:
		var size uint16
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeMap(buf, int(size))
	case marker == Map32Marker:
		var size uint32
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeMap(buf, int(size))

	// Structure
	case marker >= TinyStructMarker && marker <= TinyStructMarker+0x0F:
		size := int(marker - TinyStructMarker)
		return d.decodeStruct(buf, size)
	case marker == Struct8Marker:
		var size uint8
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeStruct(buf, int(size))
	case marker == Struct16Marker:
		var size uint16
		if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeStruct(buf, int(size))

	default:
		return nil, fmt.Errorf("unknown marker: 0x%02X", marker)
	}
}

func (d *Decoder) decodeSlice(buf *bytes.Buffer, size int) ([]interface{}, error) {
	slice := make([]interface{}, size)
	for i := 0; i < size; i++ {
		val, err := d.decodeValue(buf)
		if err != nil {
			return nil, err
		}
		slice[i] = val
	}
	return slice, nil
}

func (d *Decoder) decodeMap(buf *bytes.Buffer, size int) (map[string]interface{}, error) {
	m := make(map[string]interface{}, size)
	for i := 0; i < size; i++ {
		keyVal, err := d.decodeValue(buf)
		if err != nil {
			return nil, err
		}
		key, ok := keyVal.(string)
		if !ok {
			return nil, fmt.Errorf("map key must be string, got %T", keyVal)
		}

		val, err := d.decodeValue(buf)
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

// RawStruct represents a decoded structure with signature and fields
type RawStruct struct {
	Sig    byte
	Fields []interface{}
}

func (d *Decoder) decodeStruct(buf *bytes.Buffer, numFields int) (*RawStruct, error) {
	sig, err := buf.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read struct signature: %w", err)
	}

	fields := make([]interface{}, numFields)
	for i := 0; i < numFields; i++ {
		val, err := d.decodeValue(buf)
		if err != nil {
			return nil, err
		}
		fields[i] = val
	}

	return &RawStruct{Sig: sig, Fields: fields}, nil
}
