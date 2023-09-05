package btree

const (
	PAGE_CACHE_FETCH uint8 = 0x1 // only fetch a page cache
	PAGE_CACHE_CREAT uint8 = 0x2 // create a page cache
)

type Pager interface {
	FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error)
	// Insert(pageNo PageNumber, data []byte) error

	GetPageNumber() PageNumber
}

type pager struct {
	PageCache  PageCache  // page cache interface
	PageNumber PageNumber // page number in the database file
}

// FetchPage fetch a page from pager.
// if the page already in the page cache, return the cache directly.
// if there is a cache miss and PAGE_CACHE_CREAT flag is set, create a new page.
// otherwise return nil
func (pgr *pager) FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error) {
	if pageNo == 0 {
		return nil, errorInvalidPageNumber
	}
	pce, err := pgr.PageCache.FetchPage(pageNo, flag)
	if err != nil {
		return nil, err
	}
	return pce, nil
}

func (pgr *pager) GetPageNumber() PageNumber {
	return pgr.PageNumber
}
