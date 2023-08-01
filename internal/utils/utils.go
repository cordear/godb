package utils

import (
	"bytes"
	"encoding/binary"
)

func SetUint16(raw []byte, data uint16) {
	buf := bytes.NewBuffer(raw)
	buf.Reset()
	binary.Write(buf, binary.LittleEndian, data)
}

func SetUint32(raw []byte, data uint32) {
	buf := bytes.NewBuffer(raw)
	buf.Reset()
	binary.Write(buf, binary.LittleEndian, data)
}
