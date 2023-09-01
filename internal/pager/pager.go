package pager

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

func (pgr *pager) FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error) {
	return pgr.PageCache.FetchPage(pageNo, flag)
}

func (pgr *pager) GetPageNumber() PageNumber {
	return pgr.PageNumber
}

// function used for test
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
