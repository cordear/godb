package btree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// dump a mempage content to file
func DumpToFile(mem Mempage) {
	f, err := os.Create("test.db")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := f.Write(mem.RawData); err != nil {
		panic(err)
	}
}
func NewPager() pager {
	var pager pager
	var pcache pageCache
	pcache.cacheHash = make(map[PageNumber]*PageCacheEntry)
	// generate page one for test
	pageOne, err := NewMemPage(1, PAGE_DATA|PAGE_LEAF|PAGE_LEAF_DATA)
	if err != nil {
		panic(err)
	}
	pcache.cacheHash[1] = &PageCacheEntry{PageNo: 1, Dirty: true, Data: pageOne}
	pcache.pager = &pager
	pager.PageCache = &pcache
	pager.PageNumber = 1
	return pager
}

func TestSerialInsert(t *testing.T) {
	pager := NewPager()
	bs := BtreeShared{}
	bs.Pager = &pager
	btree := btree{}
	btree.Shared = &bs
	cursor := btCursor{}
	cursor.Btree = &btree
	cursor.RootPageNo = 1
	cursor.Insert(2, []byte{0x4, 0x5, 0x6})         // 1
	cursor.Insert(1, []byte{0x1, 0x2, 0x3})         // 0
	cursor.Insert(3, []byte{0x7, 0x8, 0x9})         // 2
	cursor.Insert(9, []byte{0x7, 0x8, 0x6})         // 6
	cursor.Insert(6, []byte{0x7, 0x8, 0x4})         // 4
	cursor.Insert(7, []byte{0x7, 0x8, 0x5})         // 5
	cursor.Insert(11, []byte{0x7, 0x8, 0x7})        // 7
	cursor.Insert(13, []byte{0x13, 0x8, 0x9, 0x13}) // 8
	cursor.Insert(4, []byte{0x4, 0x8, 0x9, 0x15})   // 3
	cellOne := cursor.Mem.GetKthCell(0)
	cellTwo := cursor.Mem.GetKthCell(3)
	cellThree := cursor.Mem.GetKthCell(8)
	DumpToFile(*cursor.Mem)
	assert.Equal(t, cellOne.Payload, []byte{0x1, 0x2, 0x3})
	assert.Equal(t, cellTwo.Payload, []byte{0x4, 0x8, 0x9, 0x15})
	assert.Equal(t, cellThree.Payload, []byte{0x13, 0x8, 0x9, 0x13})
}
