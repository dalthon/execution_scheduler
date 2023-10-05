package execution_scheduler

import "container/heap"

// Attention: executionQueue is not thread safe!
type executionQueue struct {
	queue priorityQueue
}

func newExecutionQueue() *executionQueue {
	return &executionQueue{
		queue: make(priorityQueue, 0),
	}
}

func (queue *executionQueue) Push(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
	execution := newExecution(
		handler,
		errorHandler,
		kind,
		priority,
	)

	heap.Push(&queue.queue, execution)

	return execution
}

func (queue *executionQueue) Size() int {
	return len(queue.queue)
}

func (queue *executionQueue) Top() *Execution {
	if len(queue.queue) == 0 {
		return nil
	}

	return queue.queue[0]
}

func (queue *executionQueue) Pop() *Execution {
	if len(queue.queue) == 0 {
		return nil
	}

	return (heap.Pop(&queue.queue).(*Execution))
}

func (queue *executionQueue) PopPriority(priority int) *Execution {
	if len(queue.queue) == 0 || queue.Top().priority <= priority {
		return nil
	}

	return (heap.Pop(&queue.queue).(*Execution))
}

func (queue *executionQueue) Remove(execution *Execution) {
	if execution.index != -1 {
		heap.Remove(&queue.queue, execution.index)
	}
}

type priorityQueue []*Execution

func (queue priorityQueue) Len() int { return len(queue) }

func (queue priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest priority, not lowest
	// Then we return greatest instead of lowest here
	return queue[i].priority > queue[j].priority
}

func (queue priorityQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
	queue[i].index = i
	queue[j].index = j
}

func (queue *priorityQueue) Push(item any) {
	n := len(*queue)
	execution := item.(*Execution)
	execution.index = n
	*queue = append(*queue, execution)
}

func (queue *priorityQueue) Pop() any {
	oldQueue := *queue
	size := len(oldQueue)
	execution := oldQueue[size-1]
	oldQueue[size-1] = nil
	execution.index = -1
	*queue = oldQueue[0 : size-1]

	return execution
}
