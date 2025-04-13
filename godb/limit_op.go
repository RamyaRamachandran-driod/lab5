package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	//<strip lab1|lab2>
	return &LimitOp{child, lim}
	//</strip>
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	//<strip lab1|lab2>
	return l.child.Descriptor()
	//</strip>
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	//<strip lab1|lab2>
	numTupsExpr, err := l.limitTups.EvalExpr(&Tuple{})
	if err != nil {
		return nil, err
	}
	limitTups := numTupsExpr.(IntField).Value
	var numTups int64
	childIt, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		t, err := childIt()
		if err != nil {
			return nil, err
		}
		if t == nil || numTups == limitTups {
			return nil, nil
		}
		numTups++
		return t, nil
	}, nil
	//</strip>
}
