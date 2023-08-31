package btree

import (
	"godb/internal/pager"
	"testing"

	"github.com/stretchr/testify/assert"
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
	cursor.Insert(9, []byte{0x7, 0x8, 0x9})
	cursor.Insert(6, []byte{0x7, 0x8, 0x9})
	cursor.Insert(7, []byte{0x7, 0x8, 0x9})
	cursor.Insert(11, []byte{0x7, 0x8, 0x9})
	cursor.Insert(13, []byte{0x13, 0x8, 0x9, 0x13})
	cursor.Insert(4, []byte{0x4, 0x8, 0x9, 0x15})
	cellOne := cursor.Mem.GetKthCell(0)
	cellTwo := cursor.Mem.GetKthCell(3)
	cellThree := cursor.Mem.GetKthCell(8)
	assert.Equal(t, cellOne.Payload, []byte{0x1, 0x2, 0x3})
	assert.Equal(t, cellTwo.Payload, []byte{0x4, 0x8, 0x9, 0x15})
	assert.Equal(t, cellThree.Payload, []byte{0x13, 0x8, 0x9, 0x13})
}
