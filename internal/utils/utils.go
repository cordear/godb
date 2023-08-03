package utils

import (
	"bytes"
	"encoding/binary"
)

func SetUint16(raw []byte, data uint16) {
	buf := bytes.NewBuffer([]byte{})
	buf.Reset()
	binary.Write(buf, binary.LittleEndian, data)
	for i, v := range buf.Bytes() {
		raw[i] = v
	}
}

func GetUint16(raw []byte) uint16 {
	return binary.LittleEndian.Uint16(raw[:2])
}

func SetUint32(raw []byte, data uint32) {
	buf := bytes.NewBuffer([]byte{})
	buf.Reset()
	binary.Write(buf, binary.LittleEndian, data)
	for i, v := range buf.Bytes() {
		raw[i] = v
	}
}

func GetUint32(raw []byte) uint32 {
	return binary.LittleEndian.Uint32(raw[:4])
}
