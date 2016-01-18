package zoothrift

import (
	"sync"
)

const minQueueLen = 16

// Queue represents a single instance of the queue data structure.
type Queue struct {
	buf               []interface{}
	head, tail, count int
	lock              sync.Mutex
}

// New constructs and returns a new Queue.
func NewPool() *Queue {
	return &Queue{
		buf: make([]interface{}, minQueueLen),
	}
}

// Length returns the number of elements currently stored in the queue.
func (q *Queue) Length() int {
	return q.count
}

// resizes the queue to fit exactly twice its current contents
// this can result in shrinking if the queue is less than half-full
func (q *Queue) resize() {
	newBuf := make([]interface{}, q.count*2)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// Add puts an element on the end of the queue.
func (q *Queue) Add(elem interface{}) {
	q.lock.Lock()
	if q.count == len(q.buf) {
		q.resize()
	}
	q.buf[q.tail] = elem
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
	q.lock.Unlock()
}

// Peek returns the element at the head of the queue. This call panics
// if the queue is empty.
func (q *Queue) Peek() interface{} {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.count <= 0 {
		panic("queue: Peek() called on empty queue")
	}
	item := q.buf[q.head]
	q.count--
	return item
}

// Get returns the element at index i in the queue. If the index is
// invalid, the call will panic.
func (q *Queue) Get(i int) interface{} {
	if i < 0 || i >= q.count {
		panic("queue: Get() called with index out of range")
	}
	return q.buf[(q.head+i)%len(q.buf)]
}

// Remove removes the element from the front of the queue. If you actually
// want the element, call Peek first. This call panics if the queue is empty.
func (q *Queue) Remove() {
	q.lock.Lock()
	if q.count <= 0 {
		panic("queue: Remove() called on empty queue")
	}
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	if len(q.buf) > minQueueLen && q.count*4 == len(q.buf) {
		q.resize()
	}
	q.lock.Unlock()
}
