package pager

type PageNumber uint32

type PageCache interface {
	FetchPage(pageNo PageNumber, flag uint8) *PageCacheEntry
}

type PageCacheEntry struct {
	pageNo PageNumber // the page number of the cache entry
	dirty  bool       // true if the data in the cache is modified
	data   []byte     // the cached page data
}

type pageCache struct {
	pager     *Pager                         // pager that own the page cache object
	cacheHash map[PageNumber]*PageCacheEntry // page cache hash, store the page cache entry pointer
}

func (pcache *pageCache) FetchPage(pageNo PageNumber, flag uint8) *PageCacheEntry {
	if entry, ok := pcache.cacheHash[pageNo]; ok {
		return entry
	} else if (flag & PAGE_CACHE_CREAT) == 1 {
		newPageNo := (*pcache.pager).GetPageNumber() + 1
		pce := new(PageCacheEntry)
		pce.data = make([]byte, 4096)
		pce.dirty = false
		pcache.cacheHash[newPageNo] = pce
		return pce
	}
	return nil
}
