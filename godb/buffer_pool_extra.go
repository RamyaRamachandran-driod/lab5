package godb

import (
	"fmt"
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
	return fmt.Errorf("not implemented") // replace it
}
