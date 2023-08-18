package btree

import (
	"godb/internal/pager"
	"testing"
)

func TestSerialInsert(t *testing.T) {
	pager := pager.NewPager()
	bs := BtreeShared{}
	bs.Pager = &pager
	btree := btree{}
	btree.Shared = &bs
	cursor := btCursor{}
	cursor.Btree = &btree
	cursor.RootPageNo = 2
	cursor.Insert(2, []byte{0x4, 0x5, 0x6})
	cursor.Insert(1, []byte{0x1, 0x2, 0x3})
	cursor.Insert(3, []byte{0x7, 0x8, 0x9})
	cellOne := cursor.Mem.GetKthCell(0)
	cellTwo := cursor.Mem.GetKthCell(1)
	cellThree := cursor.Mem.GetKthCell(2)
	t.Log(cellOne.RawData)
	t.Log(cellOne.Payload)
	t.Log(cellTwo.RawData)
	t.Log(cellTwo.Payload)
	t.Log(cellThree.RawData)
	t.Log(cellThree.Payload)
}
