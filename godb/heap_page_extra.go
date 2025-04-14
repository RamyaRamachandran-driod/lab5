package godb

// Returns the before-image of the page. This is used for logging and recovery.
func (p *heapPage) BeforeImage() Page {
	// TODO: some code goes here
	if p.beforeImage != nil {
		return p.beforeImage
	}
	// Create a copy manually.
	newPg := *p
	// Clear the beforeImage of the copy
	newPg.beforeImage = nil
	// Copy the tuples slice so that changes to the original won't affect the copy.
	if p.tuples != nil {
		newPg.tuples = make([]*Tuple, len(p.tuples))
		copy(newPg.tuples, p.tuples)
	}
	return &newPg
}

// Sets the before-image of the page to the current state of the page. Be sure
// that changing the page does not change the before-image.
func (p *heapPage) SetBeforeImage() {
	// TODO: some code goes here
	// Create a copy of the current page.
	newPg := *p
	// Set the beforeImage field of the copy to nil
	newPg.beforeImage = nil
	// Copy the tuples slice.
	if p.tuples != nil {
		newPg.tuples = make([]*Tuple, len(p.tuples))
		copy(newPg.tuples, p.tuples)
	}
	p.beforeImage = &newPg
}

// Returns the page number of the page.
func (p *heapPage) PageNo() int {
	// TODO: some code goes here
	return p.pageNo
}
