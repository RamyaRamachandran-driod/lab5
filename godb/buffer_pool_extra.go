package godb

import (
	"fmt"
)

// Rolls back a transaction by reading the log and undoing the changes made by
// the transaction.
func (bp *BufferPool) Rollback(tid TransactionID) error {
	// TODO: some code goes here
	return fmt.Errorf("not implemented") // replace it
}

// Returns the log file associated with the buffer pool.
func (bp *BufferPool) LogFile() *LogFile {
	// TODO: some code goes here
	return nil // replace it
}

// Recover the buffer pool from a log file. This should be called when the
// database is started, even if the log file is empty.
func (bp *BufferPool) Recover(logFile *LogFile) error {
	// TODO: some code goes here
	return fmt.Errorf("not implemented") // replace it
}
