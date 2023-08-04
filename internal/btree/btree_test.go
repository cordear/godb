package btree

import (
	"godb/internal/pager"
	"testing"
)

func TestInsert(t *testing.T) {
	pager := pager.NewPager()
	bs := BtreeShared{}
	bs.Pager = &pager
	btree := btree{}
	btree.Shared = &bs
	cursor := btCursor{}
	cursor.Btree = &btree
	cursor.RootPageNo = 2
	cursor.Insert(1, []byte{0x1, 0x2, 0x3})
	cell := cursor.Mem.GetKthCell(0)
	t.Log(cell.RawData)
	t.Log(cell.Payload)
}
