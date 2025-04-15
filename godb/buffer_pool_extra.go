package godb

import (
	"fmt"
	"io"
)

// Rolls back a transaction by reading the log and undoing the changes made by
// the transaction.
// Rollback undoes all changes made by the transaction identified by tid.
func (bp *BufferPool) Rollback(tid TransactionID) error {
	iter, err := bp.logFile.ReverseIterator()
	if err != nil {
		return fmt.Errorf("failed to create reverse iterator: %w", err)
	}
	for {
		record, err := iter()
		if err != nil {
			return fmt.Errorf("error reading log record: %w", err)
		}
		if record == nil {
			break
		}
		if record.Tid() != tid {
			continue
		}

		if updateRec, ok := record.(*UpdateLogRecord); ok {
			beforePage := updateRec.Before

			if err := beforePage.getFile().flushPage(beforePage); err != nil {
				return fmt.Errorf("failed to flush page during rollback: %w", err)
			}
			beforePage.setDirty(-1, false)
		}
	}

	return nil
}

// Returns the log file associated with the buffer pool.
func (bp *BufferPool) LogFile() *LogFile {
	// TODO: some code goes here
	return bp.logFile
}

// Recover the buffer pool from a log file. This should be called when the
// database is started, even if the log file is empty.
func (bp *BufferPool) Recover(logFile *LogFile) error {
	// TODO: some code goes here
	bp.logFile = logFile

	if err := bp.logFile.seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of file: %w", err)
	}

	losers := make(map[TransactionID]int64)
	iter := bp.logFile.ForwardIterator()
	record, err := iter()
	for record != nil && err == nil {
		fmt.Printf("Recovering record %+v\n", record)
		switch record.Type() {
		case BeginRecord:
			losers[record.Tid()] = record.Offset()
		case AbortRecord:
		case CommitRecord:
			delete(losers, record.Tid())
		case UpdateRecord:
			updateRecord := record.(*UpdateLogRecord)

			after := updateRecord.After.(*heapPage)
			pageKey := after.getFile().pageKey(after.PageNo())
			fmt.Printf("REDO %v", pageKey)
			delete(bp.pages, pageKey)
			if err := after.getFile().flushPage(after); err != nil {
				return err
			}
		}
		record, err = iter()
	}
	if err != nil {
		return err
	}

	iter, err = bp.logFile.ReverseIterator()
	if err != nil {
		return fmt.Errorf("failed to create rev iterator: %w", err)
	}
	record, err = iter()
	for len(losers) > 0 && record != nil && err == nil {
		tid := record.Tid()
		_, is_loser := losers[tid]
		if is_loser {
			switch record.Type() {
			case UpdateRecord:
				updateRecord := record.(*UpdateLogRecord)
				page := updateRecord.Before.(*heapPage)
				pageKey := page.getFile().pageKey(page.PageNo())
				fmt.Printf("UNDO %v", pageKey)
				delete(bp.pages, pageKey)
				if err := page.getFile().flushPage(page); err != nil {
					return err
				}
			case BeginRecord:
				offset := bp.logFile.offset
				if err := bp.logFile.seek(0, io.SeekEnd); err != nil {
					return err
				}
				bp.logFile.LogAbort(tid)
				if err := bp.logFile.Force(); err != nil {
					return err
				}
				if err := bp.logFile.seek(offset, io.SeekStart); err != nil {
					return err
				}
				delete(losers, tid)
			}
			// abortTIDs = append(abortTIDs, tid)
    		// delete(losers, tid)
			
		}
		record, err = iter()
	}
	if err != nil {
		return fmt.Errorf("failed to read from reversed iterator: %w", err)
	}
	return bp.logFile.seek(0, io.SeekEnd)
}
