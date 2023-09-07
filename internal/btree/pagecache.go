package btree

import (
	"errors"
)

var (
	ErrorCacheMiss = errors.New("cache missed")
)

type PageCache interface {
	FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error)
}

type PageCacheEntry struct {
	PageNo PageNumber // the page number of the cache entry
	Dirty  bool       // true if the data in the cache is modified
	Data   *MemPage   // the cached page data
}

type pageCache struct {
	pager     *pager                         // pager that own the page cache object
	cacheHash map[PageNumber]*PageCacheEntry // page cache hash, store the page cache entry pointer
}

func (pcache *pageCache) FetchPage(pageNo PageNumber, flag uint8) (*PageCacheEntry, error) {
	// TODO: finish the fetch page logic
	if entry, ok := pcache.cacheHash[pageNo]; ok {
		return entry, nil
	} else if (flag & PAGE_CACHE_CREAT) > 0 {
		// cache miss, try to create a new page and return
		newPageNo := (*pcache.pager).GetPageNumber() + 1
		(*pcache.pager).PageNumber += 1
		pce := new(PageCacheEntry)
		pce.Data, _ = NewZeroPage(newPageNo)
		pce.Dirty = true
		pce.PageNo = newPageNo
		// add the newly created page into page cache
		pcache.cacheHash[newPageNo] = pce
		return pce, nil
	}
	return nil, ErrorCacheMiss
}

// ToMemPage return the MemPage the PageCacheEntry hold
// if the MemPage not init before, ToMemPage will init the MemPage's PageNo, BShared nad HeaderOffset field
func (pce PageCacheEntry) ToMemPage(pageNo PageNumber, shared *Shared) *MemPage {
	mem := pce.Data
	if mem.PageNo != pageNo {
		// if the page's page number != given page number, then the page is newly created.
		// thus need to init the page content.
		mem.PageNo = pageNo
		mem.BShared = shared
		if pageNo == 1 {
			mem.HeaderOffset = 100
		} else {
			mem.HeaderOffset = 0
		}
	}
	return mem
}
