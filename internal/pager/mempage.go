package pager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/internal/utils"
)

var (
	ErrorInvaildPageType = errors.New("invaild page type")
)

// each page has a unique page number that must bigger than 0.
// page number zero means no such page.
type PageNumber uint32

// The flag used in page header. A vaild flag must be one of:
// - PAGE_INDEX
// - PAGE_INDEX | PAGE_LEAF
// - PAGE_DATA | PAGE_LEAF_DATA
// - PAGE_DATA | PAGE_LEAF_DATA | PAGE_LEAF
const (
	PAGE_DATA      uint8 = 0x01 // a table b-tree page
	PAGE_INDEX     uint8 = 0x02 // a index b-tree page
	PAGE_LEAF_DATA uint8 = 0x04 // a table b-tree leaf page
	PAGE_LEAF      uint8 = 0x08 // a leaf page
)

// Page header layout:
//
// OFFSET	SIZE	DATA
//    0       1     flags
//    1       2     number of cells
//    3       5     reserved
//    8       4     right child page number. only used in non-leaf page

// A page in memory
type Mempage struct {
	IsInit            bool       // true if init before, false if need reinit
	PageNo            PageNumber // page number
	IsDataPage        bool       // true if table b-tree. false if index b-tree
	IsDataLeaf        bool       // true if table b-tree leaf. false otherwise
	IsLeaf            bool       // true if leaf page. false otherwise
	IsPageOne         bool       // true if page 1. false otherwise
	CellNum           uint16     // number of cell inside the page
	RawData           []byte     // raw data of the page
	CellIndexOffset   uint16     // offset for cell index
	CellContentOffset uint16     // offset for cell content, only meaningful for leaf page
}

// an in memory cell
type Cell struct {
	LeftChildPageNo PageNumber // left child page number
	Payloadsize     uint16     // the payload size, exclude the key
	Key             uint32     // key
	Payload         []byte     // pointer to the payload
}

func NewCell(key uint32, payload []byte) Cell {
	var cell Cell
	cell.LeftChildPageNo = 0
	cell.Key = key
	cell.Payloadsize = uint16(len(payload))
	return cell
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

func NewMemPage(pageNo PageNumber, flag uint8) (*Mempage, error) {
	mem := new(Mempage)
	raw := make([]byte, 4096)
	if !checkFlag(flag) {
		return nil, errors.New("invaild flag")
	}
	mem.RawData = raw
	mem.IsLeaf = bool((flag & PAGE_LEAF) != 0)
	mem.IsDataPage = bool((flag & PAGE_DATA) != 0)
	mem.IsDataLeaf = bool(mem.IsDataPage && mem.IsLeaf)
	mem.CellNum = 0
	mem.PageNo = pageNo
	if mem.PageNo == 1 {
		mem.IsPageOne = true
	} else {
		mem.IsPageOne = false
	}
	mem.IsInit = true
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

// only the non-leaf child has a right child
func (mem *Mempage) GetRightChild() PageNumber {
	if mem.IsLeaf {
		return 0
	}
	offset := 0
	if mem.IsPageOne {
		offset += 100
	}
	return PageNumber(utils.GetUint32(mem.RawData[offset+8:]))
}

func (mem *Mempage) GetKthCellIndex(k uint16) uint16 {
	return utils.GetUint16(mem.RawData[mem.CellIndexOffset+k*2:])
}

func (mem *Mempage) GetKthLeftPageNumber(k uint16) PageNumber {
	offset := mem.GetKthCellIndex(k) - 4
	return PageNumber(utils.GetUint32(mem.RawData[offset:]))
}

func (mem *Mempage) GetKthCellSize(k uint16) uint16 {
	offset := mem.GetKthCellIndex(k) - 6
	return utils.GetUint16(mem.RawData[offset:])
}

func (mem *Mempage) GetKthKey(k uint16) uint32 {
	offset := mem.GetKthCellIndex(k) - 10
	return utils.GetUint32(mem.RawData[offset:])
}

func (mem *Mempage) GetKthCellContent(k uint16) ([]byte, uint16) {
	offset := mem.GetKthCellIndex(k)
	size := mem.GetKthCellSize(k)
	return mem.RawData[offset-10-size:], size
}

func (mem *Mempage) WriteCellContent(key uint32, data []byte) error {

	return nil
}

func (mem *Mempage) GetKthCell(k uint16) Cell {
	offset := mem.GetKthCellIndex(k)
	size := mem.GetKthCellSize(k)
	leftChild := mem.GetKthLeftPageNumber(k)
	key := mem.GetKthKey(k)
	return Cell{LeftChildPageNo: leftChild,
		Payloadsize: size,
		Key:         key,
		Payload:     mem.RawData[offset:]}
}

func (mem *Mempage) InsertCellFast(cell Cell, i uint16) {
	// convert cell to raw bytes
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, cell.LeftChildPageNo)
	binary.Write(buf, binary.LittleEndian, cell.Payloadsize)
	binary.Write(buf, binary.LittleEndian, cell.Key)
	binary.Write(buf, binary.LittleEndian, cell.Payload)
	// TODO: finish the remaining task for insert
}
