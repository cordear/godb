package btree

import (
	"errors"
)

var (
	ErrorInvalidPageNumber = errors.New("invalid page number")
	ErrorCorruptedPage     = errors.New("page corrupted")
	ErrorInvalidFlags      = errors.New("invalid flags")
)

type Btree interface {
	Insert(key uint32, data []byte) error
}

type BtCursor interface {
	Insert(key uint32, data []byte) error
	MoveToRoot() error
	MoveTo(key uint32)
	MoveNext() error
	MoveToParent() error
	MovetoChild(pageNo PageNumber)
	CompareKey(key uint32) int8
}

type btree struct {
	Shared *Shared // shared content
}

type btCursor struct {
	Btree             *btree     // btree that own the cursor
	Mem               *MemPage   // the in memory page the cursor point to
	CellIndex         uint16     // cell index the cursor point to
	RootPageNo        PageNumber // btree root page number
	LastCompareResult int8       // last compare result
	PStack            []*MemPage // stack for parents of current page
}

// Shared is the sharable content of the btree
type Shared struct {
	Pager      Pager      // the page cache
	PageOne    MemPage    // first page of the database, always in memory
	BtCursor   []BtCursor // current opened cursor on the btree
	NumPage    uint32     // number of page in the database
	UsableSize uint32     // the usable bytes on each page associate with the btree
}

//	func (bt *btree) Insert(key uint32, data []byte) error {
//		pte, err := bt.Shared.Pager.FetchPage(bt.rootPageNo, pager.PAGE_CACHE_CREAT|pager.PAGE_CACHE_FETCH)
//		if err != nil {
//			return err
//		}
//		root := pte.Data
//		loc := root.BinarySearchKey(key)
//	}

// GetPage get a page from the pager.
func (bs *Shared) GetPage(pageNo PageNumber, flags uint8) (*MemPage, error) {
	pce, err := bs.Pager.FetchPage(pageNo, flags)
	if err != nil {
		return nil, err
	}
	// TODO: finish get page logic
	return pce.ToMemPage(pageNo, bs), nil
}

// AllocateNewPage will allocate a new page from the database file.
func (bs *Shared) AllocateNewPage() (*MemPage, error) {
	// FIXME: currently, the AllocateNewPage always allocate a new page rather than use free list
	// because three are no free page management.
	bs.NumPage += 1
	mem, err := bs.GetPage(PageNumber(bs.NumPage), PAGE_CACHE_FETCH|PAGE_CACHE_CREAT)
	if err != nil {
		return nil, err
	}
	return mem, nil
}

func (btc *btCursor) Insert(key uint32, data []byte) error {
	// move to the proper position
	loc, err := btc.MoveTo(key)
	if err != nil {
		return err
	}
	cell := NewCell(key, data)
	if loc == 0 { // if loc == 0, then the cursor is in the key itself
		cell.LeftChildPageNo = btc.Mem.GetKthLeftPageNumber(btc.CellIndex)
	} else if loc > 0 && btc.Mem.CellNum > 0 {
		// the cursor point to a value bigger than the key.
		// The key will insert on the left side

	} else if loc < 0 {
		// the cursor point to a value smaller than the key,
		// The key will insert on the right side
		btc.CellIndex++
	}
	// TODO: finish the cursor insert
	err = btc.Mem.InsertCellFast(cell, btc.CellIndex)
	if err != nil {
		return nil
	}
	// insert produce at least one overflow cell, which means the page is full.
	// the page thus need a balance.
	if len(btc.Mem.OverflowCell) > 0 {
		err = btc.balance()
		if err != nil {
			return err
		}
	}
	return nil
}

// balance the page the cursor currently point to
func (btc *btCursor) balance() error {
	mem := btc.Mem
	if len(mem.OverflowCell) == 0 &&
		mem.FreeBytes*3 <= 4096*2 {
		// if page has no overflow cell and page has enough space,
		// there is no need to balance.
		return nil
	} else if len(btc.PStack) == 0 {
		// the root page need balance
		rightChild, err := mem.BalanceDeep()
		if err != nil {
			return err
		}
		btc.PStack = append(btc.PStack, rightChild)
	}
	return nil
}

// MoveToRoot move to the root page of the btree
func (btc *btCursor) MoveToRoot() error {
	// get the root page
	rootMem, err := btc.Btree.Shared.GetPage(btc.RootPageNo, PAGE_CACHE_FETCH|PAGE_CACHE_CREAT)
	if err != nil {
		return err
	}
	btc.Mem = rootMem
	btc.CellIndex = 0
	// clean the parents stack
	btc.PStack = nil
	return nil
}

// MoveTo move the cursor to a proper position relate to the key.
// return value > 0 if cursor point to a value bigger than the search key or cursor on an empty page
// return value = 0 if cursor point to exact the same key
// return value < 0 if cursor point to a value smaller than the search key
func (btc *btCursor) MoveTo(key uint32) (int8, error) {
	// reset the cursor to root page, the CellIndex is set to 0.
	err := btc.MoveToRoot()
	if err != nil {
		return -2, err
	}
	// the page is empty, directly return
	if btc.Mem.CellNum == 0 {
		return 1, nil
	}
	var lo int32 = 0
	var hi = int32(btc.Mem.CellNum) - 1
	var child PageNumber = 0
	var c int8 = -1
	// binary search the content index array
	for {
		for lo <= hi {
			btc.CellIndex = uint16(lo + (hi-lo)/2)
			c = btc.CompareKey(key)
			// if c > 0, which means cursorKey < key
			if c > 0 {
				hi = int32(btc.CellIndex) - 1
			} else if c == 0 {
				btc.LastCompareResult = c
				return c, nil
			} else {
				lo = int32(btc.CellIndex) + 1
			}
		}
		// the key is bigger than all the key in the page, move to the right child
		if lo >= int32(btc.Mem.CellNum) {
			child = btc.Mem.GetRightChild()
			// if no right child, then the cursor is point to a value than smaller than the key
			if child == 0 {
				return -1, nil
			}
		} else {
			// otherwise the cursor stop at the key that exactly bigger then the search key
			child = btc.Mem.GetKthLeftPageNumber(uint16(lo))
		}
		if child == 0 {
			btc.LastCompareResult = c
			return c, nil
		}
		err := btc.MoveToChild(child)
		if err != nil {
			return -2, err
		}
	}
}

func (btc *btCursor) MoveToChild(pageNo PageNumber) error {
	// get the child page
	childMem, err := btc.Btree.Shared.Pager.FetchPage(pageNo, PAGE_CACHE_FETCH|PAGE_CACHE_CREAT)
	if err != nil {
		return err
	}
	// before switch to the child page, push the current page into stack
	btc.PStack = append(btc.PStack, btc.Mem)
	btc.CellIndex = 0
	btc.Mem = childMem.Data
	return nil
}

func (btc *btCursor) MoveNext() error {
	btc.CellIndex++
	// check if the cursor has reached the end
	if btc.CellIndex >= btc.Mem.CellNum {
		rh := btc.Mem.GetRightChild()
		// if the right child exist
		if rh != 0 {
			err := btc.MoveToChild(rh)
			if err != nil {
				return err
			}
			err = btc.MoveToLeftMost()
			if err != nil {
				return nil
			}
			return nil
			// otherwise the cursor is in a leaf page. The cursor need to move to parent page before advance
		} else {
			for {
				// if the parent stack is empty, then the cursor can not advance anymore
				if len(btc.PStack) == 0 {
					return nil
				}
				// move to the parent page
				err := btc.MoveToParent()
				if err != nil {
					return err
				}
				// if the cursor not come from a right page of an internal page, the cursor is in correct position
				if btc.CellIndex < btc.Mem.CellNum {
					break
				}
			}
			return btc.MoveNext()
		}
	}
	err := btc.MoveToLeftMost()
	if err != nil {
		return err
	}
	return nil
}

func (btc *btCursor) MoveToLeftMost() error {
	for {
		leftPageNo := btc.Mem.GetKthLeftPageNumber(btc.CellIndex)
		if leftPageNo != 0 {
			err := btc.MoveToChild(leftPageNo)
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// CompareKey compare key to the key that cursor current point to. > 0 if
// cursorKey > key; = 0 if cursorKey = key; < 0 if cursorKey < key.
func (btc *btCursor) CompareKey(key uint32) int8 {
	cursorKey := btc.Mem.GetKthKey(btc.CellIndex)
	if cursorKey > key {
		return 1
	} else if cursorKey == key {
		return 0
	} else {
		return -1
	}
}

// MoveToParent move the cursor to the root
// the caller should guarantee there has at least one parent in the stack
func (btc *btCursor) MoveToParent() error {
	parent := btc.PStack[len(btc.PStack)-1]
	// pop the direct parent
	btc.PStack = btc.PStack[:len(btc.PStack)-1]
	// if the current page is the right page of the parent page, set the index to CellNum
	btc.CellIndex = parent.CellNum
	// find current page in the parent
	for i := 0; i < int(parent.CellNum); i++ {
		if btc.Mem.PageNo == parent.GetKthLeftPageNumber(uint16(i)) {
			btc.CellIndex = uint16(i)
			break
		}
	}
	btc.Mem = parent
	return nil
}
