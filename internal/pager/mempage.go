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

// a PageNumber is a uint32 value that indicate the location of a page in database file.
// each page has a unique page number that must bigger than 0.
// page number zero only used as function return value that means no such page.
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
//    3       2     first free block
//    5       3     reserved
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
	HeaderOffset      uint16     // 100 if page 1, otherwise 0
	CellIndexOffset   uint16     // offset for cell index
	CellContentOffset uint16     // offset for cell content, only meaningful for leaf page
}

// an in memory cell
type Cell struct {
	LeftChildPageNo PageNumber // left child page number
	Payloadsize     uint16     // the payload size, exclude the key
	Key             uint32     // key
	RawData         []byte     // pointer to the cell itself
	Payload         []byte     // pointer to payload
}

func NewCell(key uint32, payload []byte) Cell {
	var cell Cell
	cell.LeftChildPageNo = 0
	cell.Key = key
	cell.Payloadsize = uint16(len(payload))
	cell.Payload = payload
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
	mem.CellIndexOffset = first
	mem.HeaderOffset = hdr
	mem.CellContentOffset = 4096
	raw[hdr] = uint8(flag)
	utils.SetUint16(raw[hdr+1:], mem.CellNum)
	utils.SetUint16(raw[hdr+3:], first+2)
	utils.SetUint16(raw[first+4:], 4096-first-2)
	return mem, nil
}

// return the first byte of the allocated space that bigger than size
func (mem *Mempage) FindFreeSapce(size uint16) uint16 {
	freePointer := utils.GetUint16(mem.RawData[mem.HeaderOffset+3:]) // the first free block offset
	freeSize := utils.GetUint16(mem.RawData[freePointer+2:])         // the first free block size
	if freeSize > size {
		remain := freeSize - size
		utils.SetUint16(mem.RawData[freePointer+2:], remain)
		return freePointer + remain
	}
	// FIXME: this return is just a placeholder to make the complier happy
	return freePointer
}

func (mem *Mempage) AllocateSpace(size uint16) uint16 {
	offset := mem.FindFreeSapce(size)
	return offset
}

// return the right child of the page. if the page is a leaf page,
// then return 0.
func (mem *Mempage) GetRightChild() PageNumber {
	// only the non-leaf child has a right child
	if mem.IsLeaf {
		return PageNumber(0)
	}
	return PageNumber(utils.GetUint32(mem.RawData[mem.HeaderOffset+8:]))
}

func (mem *Mempage) GetKthCellIndex(k uint16) uint16 {
	return utils.GetUint16(mem.RawData[mem.CellIndexOffset+k*2:])
}

func (mem *Mempage) GetKthLeftPageNumber(k uint16) PageNumber {
	offset := mem.GetKthCellIndex(k)
	return PageNumber(utils.GetUint32(mem.RawData[offset:]))
}

func (mem *Mempage) GetKthCellSize(k uint16) uint16 {
	offset := mem.GetKthCellIndex(k) + 4
	return utils.GetUint16(mem.RawData[offset:])
}

func (mem *Mempage) GetKthKey(k uint16) uint32 {
	offset := mem.GetKthCellIndex(k) + 6
	return utils.GetUint32(mem.RawData[offset:])
}

func (mem *Mempage) GetKthCellContent(k uint16) ([]byte, uint16) {
	offset := mem.GetKthCellIndex(k)
	size := mem.GetKthCellSize(k)
	return mem.RawData[offset+10:], size
}

func (mem *Mempage) WriteCellContent(key uint32, data []byte) error {

	return nil
}

// get kth cell in the memPage
func (mem *Mempage) GetKthCell(k uint16) Cell {
	offset := mem.GetKthCellIndex(k)
	size := mem.GetKthCellSize(k)
	leftChild := mem.GetKthLeftPageNumber(k)
	key := mem.GetKthKey(k)
	return Cell{LeftChildPageNo: leftChild,
		Payloadsize: size,
		Key:         key,
		RawData:     mem.RawData[offset : offset+10+size],
		Payload:     mem.RawData[offset+10 : offset+10+size]}
}

func (mem *Mempage) InsertCellFast(cell Cell, i uint16) {
	// convert cell to raw bytes
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, cell.LeftChildPageNo)
	binary.Write(buf, binary.LittleEndian, cell.Payloadsize)
	binary.Write(buf, binary.LittleEndian, cell.Key)
	binary.Write(buf, binary.LittleEndian, cell.Payload)
	// TODO: finish the remaining task for insert

	// insert into CellIndex
	base := mem.CellIndexOffset + 2*i
	copy(mem.RawData[base+2:], mem.RawData[base:base+2*(mem.CellNum-i)])
	// insert into CellContent
	size := uint16(buf.Len())
	offset := mem.AllocateSpace(size)
	copy(mem.RawData[offset:], buf.Bytes())
	utils.SetUint16(mem.RawData[base:], offset)
	// increase CellNum in mem
	mem.CellNum += 1
	utils.SetUint16(mem.RawData[1:], mem.CellNum)

}
