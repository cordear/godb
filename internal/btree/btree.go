package btree

import (
	"godb/internal/pager"
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
	MovetoChild(pageNo pager.PageNumber)
	CompareKey(key uint32) int8
}

type btree struct {
	Shared *BtreeShared // shared content
}

type btCursor struct {
	Btree      *btree           // btree that own the curosr
	Mem        *pager.Mempage   // the in memory page the curosr point to
	CellIndex  uint16           // cell index the curosr point to
	RootPageNo pager.PageNumber // btree root page namber
	IsMatch    int8             // last compare result
	PStack     []*pager.Mempage // stack for parents of current page
}

// sharable content of the btree
type BtreeShared struct {
	Pager    pager.Pager   // the page cache
	PageOne  pager.Mempage // first page of the database, always in memory
	BtCursor []BtCursor    // current opened cursor on the btree
	NumPage  uint32        // number of page in the database
}

// func (bt *btree) Insert(key uint32, data []byte) error {
// 	pte, err := bt.Shared.Pager.FetchPage(bt.rootPageNo, pager.PAGE_CACHE_CREAT|pager.PAGE_CACHE_FETCH)
// 	if err != nil {
// 		return err
// 	}
// 	root := pte.Data
// 	loc := root.BinarySearchKey(key)
// }

func (btc *btCursor) Insert(key uint32, data []byte) error {
	// move to the proper position
	loc, err := btc.MoveTo(key)
	if err != nil {
		return err
	}
	cell := pager.NewCell(key, data)
	if loc == 0 { // if loc == 0, then the cursor is in the key itself
		cell.LeftChildPageNo = btc.Mem.GetKthLeftPageNumber(btc.CellIndex)
	} else if loc > 0 && btc.Mem.CellNum > 0 {
		// the cursor point to a value bigger than the key. The key will insert on the left side

	} else if loc < 0 {
		// the cursor point to a value smaller than the key, The key will insert on the right side
		btc.CellIndex++
	}
	// TODO: finish the cursor insert
	btc.Mem.InsertCellFast(cell, btc.CellIndex)
	return nil
}

// move to the root page of the btree
func (btc *btCursor) MoveToRoot() error {
	// get the root page
	rootMem, err := btc.Btree.Shared.Pager.FetchPage(btc.RootPageNo, pager.PAGE_CACHE_FETCH|pager.PAGE_CACHE_CREAT)
	if err != nil {
		return err
	}
	btc.Mem = rootMem.Data
	btc.CellIndex = 0
	// clear the parents stack
	btc.PStack = nil
	return nil
}

// move the cursor to a proper position relate to the key.
// return value > 0 if cursor point to a value bigger than the search key or cursor on a empty page
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
	var hi int32 = int32(btc.Mem.CellNum) - 1
	var child pager.PageNumber = 0
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
				btc.IsMatch = c
				return c, nil
			} else {
				lo = int32(btc.CellIndex) + 1
			}
		}
		// the key is bigger than all the key in the page, move to the right child
		if lo >= int32(btc.Mem.CellNum) {
			child = btc.Mem.GetRightChild()
			// if no right child, then the cursor is point to a value than smaller then the key
			if child == 0 {
				return -1, nil
			}
		} else {
			// otherwise the cursor stop at the key that exactly bigger then the search key
			child = btc.Mem.GetKthLeftPageNumber(uint16(lo))
		}
		if child == 0 {
			btc.IsMatch = c
			return c, nil
		}
		err := btc.MoveToChild(child)
		if err != nil {
			return -2, err
		}
	}
}

func (btc *btCursor) MoveToChild(pageNo pager.PageNumber) error {
	// get the child page
	childMem, err := btc.Btree.Shared.Pager.FetchPage(pageNo, pager.PAGE_CACHE_FETCH|pager.PAGE_CACHE_CREAT)
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
				// if the cursor not come from a right page of a internal page, the cursor is in correct position
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
			btc.MoveToChild(leftPageNo)
		} else {
			break
		}
	}
	return nil
}

// compare key to the key that cursor current point to.
// > 0 if cursorKey > key;
// = 0 if cursorKey = key;
// < 0 if cursorKey < key.
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

// the caller should guaratee there has at least one parent in the stack
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
