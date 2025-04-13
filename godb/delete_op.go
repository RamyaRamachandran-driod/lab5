package godb

type DeleteOp struct {
	//<strip lab1|lab2>
	child      Operator
	deleteFile DBFile
	//</strip>
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	//<strip lab1|lab2>
	return &DeleteOp{child, deleteFile}
	//</strip>
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	//<strip lab1|lab2>
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
	//</strip>

}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	//<strip lab1|lab2>
	iter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	didIterate := false
	return func() (*Tuple, error) {
		if didIterate {
			return nil, nil
		}
		cnt := 0
		for {
			t, err := iter()
			if err != nil {
				return nil, err
			}
			if t == nil {
				break
			}
			err = dop.deleteFile.deleteTuple(t, tid)
			if err != nil {
				return nil, err
			}
			cnt = cnt + 1
		}
		didIterate = true
		return &Tuple{*dop.Descriptor(), []DBValue{IntField{int64(cnt)}}, nil}, nil
	}, nil
	// </strip>
	//</strip>
}
