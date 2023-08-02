package btree

import (
	"godb/internal/pager"
)

type Btree interface {
	Insert()
}

type btree struct {
	Shared *BtreeShared
}

// sharable content for all btree within the same database file
type BtreeShared struct {
	Pager   pager.Pager // the page cache
	PageOne Mempage     // first page of the database, always in memory
	numPage uint32      // number of page in the database
}
