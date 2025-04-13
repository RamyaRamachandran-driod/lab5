package godb

import (
)

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	//<strip lab1|lab2>
	return f.child.Descriptor()
	//</strip>
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	//<strip lab1|lab2>
	childIter, err := f.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			t, error := childIter()
			if error != nil {
				return nil, error
			}
			if t == nil {
				break
			}
			val1, err := f.left.EvalExpr(t)
			if err != nil {
				return nil, error
			}
			val2, err := f.right.EvalExpr(t)
			if err != nil {
				return nil, error
			}

			if val1.EvalPred(val2, f.op) {
				return t, nil
			}
		}
		return nil, nil
	}, err
	//</strip>
}
