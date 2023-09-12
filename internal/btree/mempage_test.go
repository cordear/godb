package btree

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"godb/internal/utils"
	"testing"
)

// NewMemPage set up the MemPage using the flag passing in.
func NewMemPage(pageNo PageNumber, flag uint8) (*MemPage, error) {
	mem := new(MemPage)
	raw := make([]byte, 4096)
	if !checkFlags(flag) {
		return nil, errors.New("invalid flag")
	}
	mem.RawData = raw
	mem.IsLeaf = (flag & PAGE_LEAF) != 0
	mem.IsDataPage = (flag & PAGE_DATA) != 0
	mem.IsDataLeaf = mem.IsDataPage && mem.IsLeaf
	mem.CellNum = 0
	mem.PageNo = pageNo
	mem.FreeBytes = 4096
	if mem.PageNo == 1 {
		mem.IsPageOne = true
	} else {
		mem.IsPageOne = false
	}
	mem.IsInit = true
	hdr := uint16(0)
	var first uint16
	if mem.IsPageOne {
		first += 100
		hdr += 100
	} else {
		first = 0
	}
	if mem.IsLeaf {
		first += 8
	} else {
		first += 12
	}
	mem.FreeBytes -= first // initial free bytes is 4096 - page header length
	mem.CellIndexOffset = first
	mem.HeaderOffset = hdr
	mem.CellContentOffset = 4096
	raw[hdr] = flag
	utils.SetUint16(raw[hdr+1:], mem.CellNum)
	utils.SetUint16(raw[hdr+5:], 4096)
	mem.IsInit = true
	return mem, nil
}

func TestMemPage_ComputeFreeBytes(t *testing.T) {
	// test empty page one
	pageOne, err := NewMemPage(1, PAGE_DATA|PAGE_LEAF|PAGE_LEAF_DATA)
	if err != nil {
		t.Error(err)
	}
	err = pageOne.ComputeFreeBytes()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, pageOne.FreeBytes, uint16(4096-100-8))
	// test empty page other
	pageX, err := NewMemPage(2, PAGE_DATA|PAGE_LEAF|PAGE_LEAF_DATA)
	if err != nil {
		t.Error(err)
	}
	err = pageX.ComputeFreeBytes()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, pageX.FreeBytes, uint16(4096-8))
}
