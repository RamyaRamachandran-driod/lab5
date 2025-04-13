package godb

// Returns the before-image of the page. This is used for logging and recovery.
func (p *heapPage) BeforeImage() Page {
	// TODO: some code goes here
}

// Sets the before-image of the page to the current state of the page. Be sure
// that changing the page does not change the before-image.
func (p *heapPage) SetBeforeImage() {
	// TODO: some code goes here
}

// Returns the page number of the page.
func (p *heapPage) PageNo() int {
	// TODO: some code goes here
}
