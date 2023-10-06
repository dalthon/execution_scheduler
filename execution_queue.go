package execution_scheduler

import "container/heap"

// Attention: executionQueue is not thread safe!
type executionQueue[C any] struct {
	queue priorityQueue[C]
}

func newExecutionQueue[C any]() *executionQueue[C] {
	return &executionQueue[C]{
		queue: make(priorityQueue[C], 0),
	}
}

func (queue *executionQueue[C]) Push(handler func(C) error, errorHandler func(C, error) error, kind ExecutionKind, priority int) *Execution[C] {
	execution := newExecution(
		handler,
		errorHandler,
		kind,
		priority,
	)

	heap.Push(&queue.queue, execution)

	return execution
}

func (queue *executionQueue[C]) Size() int {
	return len(queue.queue)
}

func (queue *executionQueue[C]) Top() *Execution[C] {
	if len(queue.queue) == 0 {
		return nil
	}

	return queue.queue[0]
}

func (queue *executionQueue[C]) Pop() *Execution[C] {
	if len(queue.queue) == 0 {
		return nil
	}

	return (heap.Pop(&queue.queue).(*Execution[C]))
}

func (queue *executionQueue[C]) PopPriority(priority int) *Execution[C] {
	if len(queue.queue) == 0 || queue.Top().priority <= priority {
		return nil
	}

	return (heap.Pop(&queue.queue).(*Execution[C]))
}

func (queue *executionQueue[C]) Remove(execution *Execution[C]) {
	if execution.index != -1 {
		heap.Remove(&queue.queue, execution.index)
	}
}

type priorityQueue[C any] []*Execution[C]

func (queue priorityQueue[C]) Len() int { return len(queue) }

func (queue priorityQueue[C]) Less(i, j int) bool {
	// We want Pop to give us the highest priority, not lowest
	// Then we return greatest instead of lowest here
	return queue[i].priority > queue[j].priority
}

func (queue priorityQueue[C]) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
	queue[i].index = i
	queue[j].index = j
}

func (queue *priorityQueue[C]) Push(item any) {
	n := len(*queue)
	execution := item.(*Execution[C])
	execution.index = n
	*queue = append(*queue, execution)
}

func (queue *priorityQueue[C]) Pop() any {
	oldQueue := *queue
	size := len(oldQueue)
	execution := oldQueue[size-1]
	oldQueue[size-1] = nil
	execution.index = -1
	*queue = oldQueue[0 : size-1]

	return execution
}
