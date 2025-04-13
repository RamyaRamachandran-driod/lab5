package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	//<silentstrip lab2|lab3|lab4>
	"fmt"
	//</silentstrip>
	//<silentstrip lab1|lab2|lab3|lab4>
	"sync"
	"time"

	//</silentstrip>
	"log"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	//<strip lab1>
	pages    map[any]Page
	maxPages int
	//</strip>
	//<silentstrip lab1|lab2|lab3|lab4>
	lockTable *LockTable

	// the transactions that are currently running. This is a set, so the value
	// is not important
	runningTids map[TransactionID]any

	sync.Mutex
	//</silentstrip>

	logFile *LogFile
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	//<silentstrip lab1|lab2|lab3|lab4>
	if numPages <= 0 {
		return nil, fmt.Errorf("numPages must be positive")
	}
	bp := &BufferPool{
		make(map[any]Page),
		numPages,
		NewLockTable(),
		make(map[TransactionID]any),
		sync.Mutex{},
		nil,
	}

	return bp, nil
	//</silentstrip>
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	//<strip lab1>
	for _, page := range bp.pages {
		page.getFile().flushPage(page)
		page.setDirty(-1, false)
	}
	//</strip>
}

// <silentstrip lab1|lab2|lab3|lab4>
// Returns true if the transaction is runing.
//
// Caller must hold the bufferpool lock.
func (bp *BufferPool) tidIsRunning(tid TransactionID) bool {
	_, is_running := bp.runningTids[tid]
	return is_running
}

//</silentstrip>
// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	//<strip lab1|lab2|lab3|lab4>
	bp.Lock()
	defer bp.Unlock()

	if !bp.tidIsRunning(tid) {
		return //todo return error
	}

	if bp.logFile == nil {
		log.Printf("log file not initialized")
	}
	if err := bp.Rollback(tid); err != nil {
		log.Printf("Error rolling back transaction: %v\n", err)
	}
	bp.logFile.LogAbort(tid)
	if err := bp.logFile.Force(); err != nil {
		log.Printf("Error aborting transaction: %s\n", err)
	}

	delete(bp.runningTids, tid)

	for _, pg := range bp.lockTable.WriteLockedPages(tid) {
		delete(bp.pages, pg)
	}
	bp.lockTable.ReleaseLocks(tid)
	// </strip>
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	//<strip lab1|lab2|lab3|lab4>
	bp.Lock()
	defer bp.Unlock()

	if !bp.tidIsRunning(tid) {
		fmt.Printf("Transaction %v is not running\n", tid)
		//todo return error
		return
	}

	pages := bp.lockTable.WriteLockedPages(tid)
	for _, pg := range pages {
		page := bp.pages[pg]
		if page == nil || !page.isDirty() { //page write locked but not dirtied
			continue
		}

		pg := page.(*heapPage)
		if err := bp.logFile.LogUpdate(tid, pg.BeforeImage(), pg); err != nil {
			log.Printf("Error logging update: %v\n", err)
		}
		pg.SetBeforeImage()
	}

	bp.logFile.LogCommit(tid)
	if err := bp.logFile.Force(); err != nil {
		log.Printf("Error committing transaction: %s\n", err)
	}

	delete(bp.runningTids, tid)

	bp.lockTable.ReleaseLocks(tid)
	// </strip>
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	//<strip lab1|lab2|lab3|lab4>
	bp.Lock()
	defer bp.Unlock()

	if bp.tidIsRunning(tid) {
		return GoDBError{IllegalTransactionError, "transaction already running"}
	}
	bp.runningTids[tid] = nil

	if bp.logFile == nil {
		panic("log file not initialized")
	}
	bp.logFile.LogBegin(tid)

	//</strip>
	return nil
}

// <silentstrip lab1>
// Evict a page from the buffer pool if the pool is full.
//
// In Labs 1-4, return an error if all pages are dirty. In Lab 5, a dirty page
// may be evicted and flushed after writing an update record to the log.
func (bp *BufferPool) evictPage() error {
	if len(bp.pages) < bp.maxPages {
		return nil
	}

	// evict first clean page
	for key, page := range bp.pages {
		if !page.isDirty() {
			delete(bp.pages, key)
			return nil
		}
	}

	//<silentstrip lab1|lab2|lab3|lab4>
	// evict an arbitrary dirty page after writing an update record
	for key, page := range bp.pages {
		pg := page.(*heapPage)

		if bp.tidIsRunning(pg.dirtier) {
			if err := bp.logFile.LogUpdate(pg.dirtier, pg.BeforeImage(), pg); err != nil {
				return err
			}
			if err := bp.logFile.Force(); err != nil {
				return err
			}
		}

		page.getFile().flushPage(page)
		delete(bp.pages, key)
		return nil
	}

	return GoDBError{BufferPoolFullError, "all pages in buffer pool are dirty"}
}

//</silentstrip>
// <silentstrip lab1|lab2|lab3|lab4>
// Returns true if the transaction is runing.
func (bp *BufferPool) IsRunning(tid TransactionID) bool {
	bp.Lock()
	defer bp.Unlock()
	return bp.tidIsRunning(tid)
}

// Loads the specified page from the specified DBFile, but does not lock it.
func (bp *BufferPool) loadPage(file DBFile, pageNo int) (Page, error) {
	bp.Lock()
	defer bp.Unlock()

	hashCode := file.pageKey(pageNo)

	var pg Page
	pg, ok := bp.pages[hashCode]
	if !ok {
		var err error
		pg, err = file.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		err = bp.evictPage()
		if err != nil {
			return nil, err
		}
		bp.pages[hashCode] = pg
	}
	return pg, nil
}

//</silentstrip>
// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	//<silentstrip lab1|lab2|lab3|lab4>
	if !bp.IsRunning(tid) {
		return nil, GoDBError{IllegalTransactionError, "Transaction is not running or has aborted."}
	}

	//loop until locks are acquired
	for {
		// ensure page is in the buffer pool
		pg, err := bp.loadPage(file, pageNo)
		if err != nil {
			return nil, err
		}

		// try to lock the page
		bp.Lock()
		switch bp.lockTable.TryLock(file, pageNo, tid, perm) {
		case Grant:
			bp.Unlock()
			return pg, nil
		case Wait:
			bp.Unlock()
			time.Sleep(2 * time.Millisecond)
		case Abort:
			bp.Unlock()
			bp.AbortTransaction(tid)
			return nil, GoDBError{IllegalTransactionError, "Transaction has aborted."}
		}
	}
	// </silentstrip>
}
