package pager

import "errors"

var (
	ErrorCacheMiss = errors.New("cache missed")
)

type PageCache interface {
	FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error)
}

type PageCacheEntry struct {
	PageNo PageNumber // the page number of the cache entry
	Dirty  bool       // true if the data in the cache is modified
	Data   *Mempage   // the cached page data
}

type pageCache struct {
	pager     *Pager                         // pager that own the page cache object
	cacheHash map[PageNumber]*PageCacheEntry // page cache hash, store the page cache entry pointer
}

// TODO: finish the fetch page logic
func (pcache *pageCache) FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error) {
	if entry, ok := pcache.cacheHash[pageNo]; ok {
		return entry, nil
	} else if (flag & PAGE_CACHE_CREAT) == 1 {
		newPageNo := (*pcache.pager).GetPageNumber() + 1
		pce := new(PageCacheEntry)
		// pce.Data, _ = NewMemPage()
		pce.Dirty = true
		pcache.cacheHash[newPageNo] = pce
		return pce, nil
	}
	return nil, ErrorCacheMiss
}
