package buffer

// LRUNode represents a node in the LRU doubly linked list.
type LRUNode struct {
	buffer *Buffer
	prev   *LRUNode // toward tail
	next   *LRUNode // toward head
}

// LRUList implements an LRU tracking structure
type LRUList struct {
	head    *LRUNode // Most recently used
	tail    *LRUNode // Least recently used
	nodeMap map[*Buffer]*LRUNode
}

// NewLRUList creates a new LRU tracking list.
func NewLRUList() *LRUList {
	return &LRUList{
		nodeMap: make(map[*Buffer]*LRUNode),
	}
}

// Len returns the number of buffers in the LRU list.
func (l *LRUList) Len() int {
	return len(l.nodeMap)
}

// Add adds a buffer to the front of the LRU list.
func (l *LRUList) Add(buffer *Buffer) {
	if _, exists := l.nodeMap[buffer]; exists {
		return
	}

	node := &LRUNode{buffer: buffer}
	l.nodeMap[buffer] = node

	if l.head == nil {
		// Empty list
		l.head = node
		l.tail = node
	} else {
		// Add to front (head)
		node.prev = l.head
		l.head.next = node
		l.head = node
	}
}

// Remove removes a buffer from the LRU list.
func (l *LRUList) Remove(buffer *Buffer) {
	node, exists := l.nodeMap[buffer]
	if !exists {
		return
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.head = node.prev
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.tail = node.next
	}

	delete(l.nodeMap, buffer)
}

// MoveToFront moves a buffer to the front of the LRU list (most recently used).
func (l *LRUList) MoveToFront(buffer *Buffer) {
	node, exists := l.nodeMap[buffer]
	if !exists {
		l.Add(buffer)
		return
	}

	// Already at front
	if l.head == node {
		return
	}

	// Remove from current position
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if l.tail == node {
		l.tail = node.next
	}

	// Move to front
	node.next = nil
	node.prev = l.head
	if l.head != nil {
		l.head.next = node
	}
	l.head = node
}

// GetLRUUnpinned returns the least recently used unpinned buffer.
// Scans from tail (oldest) towards head until finding an unpinned buffer.
// Returns nil if all buffers are pinned.
func (l *LRUList) GetLRUUnpinned() *Buffer {
	node := l.tail
	for node != nil {
		if !node.buffer.IsPinned() {
			return node.buffer
		}
		node = node.next
	}
	return nil
}
