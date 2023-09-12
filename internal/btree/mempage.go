package btree

import (
	"bytes"
	"encoding/binary"
	"godb/internal/utils"
)

// PageNumber is an uint32 value that indicate the location of a page in database
// file. each page has a unique page number that must bigger than 0. page number
// zero only used as function return value that means no such page.
type PageNumber uint32

// The flag used in page header. A valid flag must be one of:
// - PAGE_INDEX
// - PAGE_INDEX | PAGE_LEAF
// - PAGE_DATA | PAGE_LEAF_DATA
// - PAGE_DATA | PAGE_LEAF_DATA | PAGE_LEAF
const (
	PAGE_DATA      uint8 = 0x01 // a table b-tree page
	PAGE_INDEX     uint8 = 0x02 // an index b-tree page
	PAGE_LEAF_DATA uint8 = 0x04 // a table b-tree leaf page
	PAGE_LEAF      uint8 = 0x08 // a leaf page
)

// Page header layout:
//
// OFFSET	SIZE	DATA
//    0       1     flags
//    1       2     number of cells
//    3       2     first free block
//    5       2     cell content offset
//    7       1     reserved
//    8       4     right child page number. only used in non-leaf page

// MemPage is  page in memory
type MemPage struct {
	IsInit            bool       // true if init before, false if the page need init
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
	FreeBytes         uint16     // free bytes in this page
	OverflowCell      []Cell     // array store overflow cell
	BShared           *Shared    // the btree shared content the MemPage belong to
}

// Cell is an in memory cell
type Cell struct {
	LeftChildPageNo PageNumber // left child page number
	PayloadSize     uint16     // the payload size, exclude the key
	Key             uint32     // key
	RawData         []byte     // pointer to the cell itself
	Payload         []byte     // pointer to payload
}

func NewCell(key uint32, payload []byte) Cell {
	var cell Cell
	cell.LeftChildPageNo = 0
	cell.Key = key
	cell.PayloadSize = uint16(len(payload))
	cell.Payload = payload
	return cell
}

func checkFlags(flag uint8) bool {
	if flag != PAGE_INDEX &&
		flag != PAGE_INDEX|PAGE_LEAF &&
		flag != PAGE_DATA|PAGE_LEAF_DATA &&
		flag != PAGE_DATA|PAGE_LEAF_DATA|PAGE_LEAF {
		return false
	}
	return true
}

// setFlags will check the flag correctness and set the MemPage field according to the flag.
func setFlags(mem *MemPage, flag uint8) error {
	if !checkFlags(flag) {
		return ErrorInvalidFlags
	}
	if (flag & PAGE_DATA) > 0 {
		mem.IsDataPage = true
	}
	if (flag & PAGE_LEAF_DATA) > 0 {
		mem.IsDataLeaf = true
	}
	if (flag & PAGE_LEAF) > 0 {
		mem.IsLeaf = true
	}
	return nil
}

// NewZeroPage create an empty page contains no data, all the fields of newly created MemPage
// is set to default value.
func NewZeroPage(pageNo PageNumber) (*MemPage, error) {
	mem := new(MemPage)
	raw := make([]byte, 4096)
	mem.RawData = raw
	return mem, nil
}

// ComputeFreeBytes will set the FreeBytes field of the MemPage
func (mem *MemPage) ComputeFreeBytes() error {
	top := mem.CellContentOffset
	cellLast := mem.CellIndexOffset + 2*mem.CellNum
	mem.FreeBytes = top - cellLast
	return nil
}

// ZeroPage will clean the MemPage content and set up the page again so that
// it appears like a clean MemPage that hold no cell.
// ZeroPage will not change PageNumber and BShared or any field relate to that.
func (mem *MemPage) ZeroPage(flags uint8) error {
	// check and set flags
	err := setFlags(mem, flags)
	if err != nil {
		return err
	}
	hdr := mem.HeaderOffset
	mem.RawData[hdr] = flags
	// clean raw data
	copy(mem.RawData[hdr:], make([]byte, 4096))

	var first = hdr
	if mem.IsLeaf {
		// leaf page has 4 bytes right child PageNumber in page header
		first += 12
	} else {
		first += 8
	}
	mem.CellIndexOffset = first
	mem.CellContentOffset = 4096
	// set cell number and first free block offset to 0
	utils.SetUint32(mem.RawData[hdr+1:], 0)
	// set cell content offset to max usable size
	utils.SetUint16(mem.RawData[hdr+5:], 4096)
	mem.FreeBytes = 4096 - first
	mem.OverflowCell = []Cell{}
	mem.CellNum = 0
	mem.IsInit = true
	return nil
}

// InitMemPage will init other field in mem so that those fields match the RawData.
// if the IsInit is set to false, InitMemPage will return ErrorCorruptedPage.
// InitMemPage will not init the FreeBytes field. the caller need another call of ComputeFreeBytes if needed.
func (mem *MemPage) InitMemPage() error {
	if mem.IsInit {
		return ErrorCorruptedPage
	}
	if mem.PageNo == 1 {
		mem.IsPageOne = true
	}
	// check and init the flags
	flags := mem.RawData[mem.HeaderOffset]
	if !checkFlags(flags) {
		return ErrorCorruptedPage
	}
	if (flags & PAGE_DATA) > 0 {
		mem.IsDataPage = true
	}
	if (flags & PAGE_LEAF) > 0 {
		mem.IsLeaf = true
	} else {
		// a leaf page header contains 4 bytes right child PageNumber
		mem.CellIndexOffset += 4
	}
	if mem.IsDataPage && mem.IsLeaf {
		mem.IsDataLeaf = true
	}
	mem.CellIndexOffset += mem.HeaderOffset + 8
	mem.CellContentOffset = utils.GetUint16(mem.RawData[mem.HeaderOffset+5:])
	mem.CellNum = utils.GetUint16(mem.RawData[mem.HeaderOffset+1:])
	mem.IsInit = true
	return nil
}

// CopyMemPage copy the content of src to dst
func CopyMemPage(dst *MemPage, src *MemPage) error {
	cellContentOffset := src.CellContentOffset
	fromHeaderOffset := src.HeaderOffset
	toHeaderOffset := 0
	if dst.PageNo == 1 {
		toHeaderOffset += 100
	}
	// copy the cellContent, page header and cellIndex array from src to dst
	copy(dst.RawData[cellContentOffset:], src.RawData[cellContentOffset:])
	copy(dst.RawData[toHeaderOffset:], src.RawData[fromHeaderOffset:src.CellIndexOffset+2*src.CellNum])
	// flag the dst as un-init
	dst.IsInit = false
	err := dst.InitMemPage()
	if err != nil {
		return err
	}
	// compute and set the FreeBytes field.
	err = dst.ComputeFreeBytes()
	if err != nil {
		return nil
	}
	return nil
}

// FindFreeSpace find a space bigger enough to hold at least size byte on the free block
func (mem *MemPage) FindFreeSpace(size uint16) uint16 {
	freePointer := utils.GetUint16(mem.RawData[mem.HeaderOffset+3:]) // the first free block offset
	freeSize := utils.GetUint16(mem.RawData[freePointer+2:])         // the first free block size
	if freeSize > size {
		remain := freeSize - size
		utils.SetUint16(mem.RawData[freePointer+2:], remain)
		return freePointer + remain
	}
	// FIXME: this return is just a placeholder to make the compiler happy
	return freePointer
}

// AllocateSpace allocate space bigger enough to hold size bytes.
// return the offset of the allocated space
func (mem *MemPage) AllocateSpace(size uint16) uint16 {
	var offset uint16 = 0                      // the return offset
	gap := mem.CellIndexOffset + 2*mem.CellNum // the first byte offset of the gap between cell index and cell content
	top := utils.GetUint16(mem.RawData[mem.HeaderOffset+5:])

	// if there is a free block, try to allocate space from free block
	if (mem.RawData[mem.HeaderOffset+3] != 0 || mem.RawData[mem.HeaderOffset+4] != 0) && gap+2 <= top {
		offset = mem.FindFreeSpace(size)
		return offset
	}
	//allocate space form the area between cell pointer array and cell content area
	top -= size
	utils.SetUint16(mem.RawData[mem.HeaderOffset+5:], top)
	mem.CellContentOffset = top
	offset = top
	return offset
}

// BalanceDeep is used when the cursor currently point to the root page and
// the root page need balance.
func (mem *MemPage) BalanceDeep() (*MemPage, error) {
	// TODO: finish balance_deep
	bShared := mem.BShared
	// allocate a new page. The new page will become the MemPage's new right child
	child, err := bShared.AllocateNewPage()
	if err != nil {
		return nil, err
	}
	// copy mem content to child
	err = CopyMemPage(child, mem)
	if err != nil {
		return nil, err
	}

	child.OverflowCell = mem.OverflowCell

	// zero root page, set child to the right child of the root page
	err = mem.ZeroPage(child.RawData[0] & ^PAGE_LEAF)
	if err != nil {
		return nil, err
	}
	utils.SetUint32(mem.RawData[mem.HeaderOffset+8:], uint32(child.PageNo))
	return nil, nil
}

// GetRightChild return the right child of the page. if the page is a leaf page,
// then return 0.
func (mem *MemPage) GetRightChild() PageNumber {
	// only the non-leaf child has a right child
	if mem.IsLeaf {
		return PageNumber(0)
	}
	return PageNumber(utils.GetUint32(mem.RawData[mem.HeaderOffset+8:]))
}

func (mem *MemPage) GetKthCellIndex(k uint16) uint16 {
	return utils.GetUint16(mem.RawData[mem.CellIndexOffset+k*2:])
}

func (mem *MemPage) GetKthLeftPageNumber(k uint16) PageNumber {
	offset := mem.GetKthCellIndex(k)
	return PageNumber(utils.GetUint32(mem.RawData[offset:]))
}

func (mem *MemPage) GetKthCellSize(k uint16) uint16 {
	offset := mem.GetKthCellIndex(k) + 4
	return utils.GetUint16(mem.RawData[offset:])
}

func (mem *MemPage) GetKthKey(k uint16) uint32 {
	offset := mem.GetKthCellIndex(k) + 6
	return utils.GetUint32(mem.RawData[offset:])
}

func (mem *MemPage) GetKthCellContent(k uint16) ([]byte, uint16) {
	offset := mem.GetKthCellIndex(k)
	size := mem.GetKthCellSize(k)
	return mem.RawData[offset+10:], size
}

func (mem *MemPage) WriteCellContent(key uint32, data []byte) error {

	return nil
}

// GetKthCell gets kth cell in the memPage
func (mem *MemPage) GetKthCell(k uint16) Cell {
	offset := mem.GetKthCellIndex(k)
	size := mem.GetKthCellSize(k)
	leftChild := mem.GetKthLeftPageNumber(k)
	key := mem.GetKthKey(k)
	return Cell{LeftChildPageNo: leftChild,
		PayloadSize: size,
		Key:         key,
		RawData:     mem.RawData[offset : offset+10+size],
		Payload:     mem.RawData[offset+10 : offset+10+size]}
}

func (mem *MemPage) InsertCellFast(cell Cell, i uint16) error {
	// convert cell to raw bytes
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, cell.LeftChildPageNo)
	binary.Write(buf, binary.LittleEndian, cell.PayloadSize)
	binary.Write(buf, binary.LittleEndian, cell.Key)
	binary.Write(buf, binary.LittleEndian, cell.Payload)
	size := uint16(buf.Len())
	// TODO: finish the remaining task for insert
	if size+2 > mem.FreeBytes {
		// the free bytes in this page can not hold the cell index + cell content
		// store the cell in the overflow array. Balance is handled in caller function
		mem.OverflowCell = append(mem.OverflowCell, cell)

	} else {
		// insert into CellIndex
		base := mem.CellIndexOffset + 2*i
		copy(mem.RawData[base+2:], mem.RawData[base:base+2*(mem.CellNum-i)])
		// insert into CellContent
		offset := mem.AllocateSpace(size)
		copy(mem.RawData[offset:], buf.Bytes())
		utils.SetUint16(mem.RawData[base:], offset)
		// increase CellNum in mem
		mem.CellNum += 1
		utils.SetUint16(mem.RawData[mem.HeaderOffset+1:], mem.CellNum)
		mem.FreeBytes -= 2 + size // 2 bytes for cell index
	}
	return nil
}
