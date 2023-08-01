package btree

import (
	"errors"
	"godb/internal/utils"
)

type pageNumber uint

// The flag used in page header. A vaild flag must be one of:
// - PAGE_INDEX
// - PAGE_INDEX | PAGE_LEAF
// - PAGE_DATA | PAGE_LEAF_DATA
// - PAGE_DATA | PAGE_LEAF_DATA | PAGE_LEAF
const (
	PAGE_DATA      = 0x01 // a table b-tree page
	PAGE_INDEX     = 0x02 // a index b-tree page
	PAGE_LEAF_DATA = 0x04 // a table b-tree leaf page
	PAGE_LEAF      = 0x08 // a leaf page
)

// Page header layout:
//
// OFFSET	SIZE	DATA
//    0       1     flags
//    1       2     number of cells
//    3       5     reserved
//    8       4     right child. only used in non-leaf page

type Mempage struct {
	PageNo            pageNumber // page number
	IsDataPage        bool       // true if table b-tree. false if index b-tree
	IsDataLeaf        bool       // true if table b-tree leaf. false otherwise
	IsLeaf            bool       // true if leaf page. false otherwise
	IsPageOne         bool       // true if page 1. false otherwise
	CellNum           uint16     // number of cell inside the page
	RawData           []byte     // raw data of the page
	CellIndexOffset   uint16     // offset for cell index
	CellContentOffset uint16     // offset for cell content, only meaningful for leaf page
}

func checkFlag(flag uint8) bool {
	if flag != PAGE_INDEX &&
		flag != PAGE_INDEX|PAGE_LEAF &&
		flag != PAGE_DATA|PAGE_LEAF_DATA &&
		flag != PAGE_DATA|PAGE_LEAF_DATA|PAGE_LEAF {
		return false
	}
	return true
}

func NewMemPage(flag uint8) (Mempage, error) {
	var mem Mempage
	raw := make([]byte, 4096)
	if !checkFlag(flag) {
		return Mempage{}, errors.New("invaild flag")
	}
	mem.RawData = raw
	mem.IsLeaf = bool((flag & PAGE_LEAF) != 0)
	mem.IsDataPage = bool((flag & PAGE_DATA) != 0)
	mem.IsDataLeaf = bool(mem.IsDataPage && mem.IsLeaf)
	mem.CellNum = 0
	mem.IsPageOne = false
	hdr := 0
	var first uint16
	if mem.IsPageOne {
		first += 100
	} else {
		first = 0
	}
	if mem.IsLeaf {
		first += 8
	} else {
		first += 12
	}
	mem.CellIndexOffset = first
	mem.CellContentOffset = 4096
	raw[hdr] = uint8(flag)
	utils.SetUint16(raw[hdr+1:], mem.CellNum)
	return mem, nil
}
