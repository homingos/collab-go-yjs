package encoding

import (
	"io"
)

// Encoder encodes data in lib0 format
type Encoder struct {
	buf []byte
}

// NewEncoder creates a new encoder
func NewEncoder() *Encoder {
	return &Encoder{buf: make([]byte, 0, 256)}
}

// Bytes returns the encoded bytes
func (e *Encoder) Bytes() []byte {
	return e.buf
}

// WriteVarUint writes a variable-length unsigned integer
func (e *Encoder) WriteVarUint(num uint64) {
	for num > 0x7f {
		e.buf = append(e.buf, byte(0x80|(num&0x7f)))
		num >>= 7
	}
	e.buf = append(e.buf, byte(num&0x7f))
}

// WriteUint8Array writes a byte array with length prefix
func (e *Encoder) WriteUint8Array(data []byte) {
	e.WriteVarUint(uint64(len(data)))
	e.buf = append(e.buf, data...)
}

// Decoder decodes data in lib0 format
type Decoder struct {
	buf []byte
	pos int
}

// NewDecoder creates a new decoder
func NewDecoder(data []byte) *Decoder {
	return &Decoder{buf: data, pos: 0}
}

// ReadVarUint reads a variable-length unsigned integer
func (d *Decoder) ReadVarUint() (uint64, error) {
	var num uint64
	var mult uint64 = 1

	for {
		if d.pos >= len(d.buf) {
			return 0, io.EOF
		}

		b := d.buf[d.pos]
		d.pos++

		num += uint64(b&0x7f) * mult
		mult *= 128

		if b < 0x80 {
			return num, nil
		}
	}
}

// ReadUint8Array reads a byte array with length prefix
func (d *Decoder) ReadUint8Array() ([]byte, error) {
	length, err := d.ReadVarUint()
	if err != nil {
		return nil, err
	}

	if d.pos+int(length) > len(d.buf) {
		return nil, io.EOF
	}

	result := make([]byte, length)
	copy(result, d.buf[d.pos:d.pos+int(length)])
	d.pos += int(length)

	return result, nil
}

// AppendVarUint appends a variable-length unsigned integer to a buffer
func AppendVarUint(buf []byte, num uint64) []byte {
	for num > 0x7f {
		buf = append(buf, byte(0x80|(num&0x7f)))
		num >>= 7
	}
	buf = append(buf, byte(num&0x7f))
	return buf
}

// ReadVarUint reads a variable-length unsigned integer from a buffer
// Returns the value and the number of bytes read
func ReadVarUint(buf []byte) (uint64, int) {
	var num uint64
	var mult uint64 = 1
	pos := 0

	for pos < len(buf) {
		b := buf[pos]
		pos++

		num += uint64(b&0x7f) * mult
		mult *= 128

		if b < 0x80 {
			return num, pos
		}
	}

	return num, pos
}
