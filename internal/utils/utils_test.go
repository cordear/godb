package utils

import "testing"

func TestSetGetUint16(t *testing.T) {
	b := make([]byte, 10)
	SetUint16(b[5:], 1234)
	n := GetUint16(b[5:])
	if n != 1234 {
		t.Error("TestSetGetUint16 failed")
	}
}

func TestSetGetUint32(t *testing.T) {
	b := make([]byte, 10)
	SetUint32(b[5:], 12345678)
	n := GetUint32(b[5:])
	if n != 12345678 {
		t.Error("TestSetGetUint32 failed")
	}
}
