package execution_scheduler

import "container/heap"

// Attention: ExecutionQueue is not thread safe!
type ExecutionQueue struct {
	queue PriorityQueue
}

func NewExecutionQueue() *ExecutionQueue {
	return &ExecutionQueue{
		queue: make(PriorityQueue, 0),
	}
}

func (queue *ExecutionQueue) Push(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
	execution := NewExecution(
		handler,
		errorHandler,
		kind,
		priority,
	)

	heap.Push(&queue.queue, execution)

	return execution
}

func (queue *ExecutionQueue) Size() int {
	return len(queue.queue)
}

func (queue *ExecutionQueue) Top() *Execution {
	if len(queue.queue) == 0 {
		return nil
	}

	return queue.queue[0]
}

func (queue *ExecutionQueue) Pop() *Execution {
	if len(queue.queue) == 0 {
		return nil
	}

	return (heap.Pop(&queue.queue).(*Execution))
}

func (queue *ExecutionQueue) PopPriority(priority int) *Execution {
	if len(queue.queue) == 0 || queue.Top().priority < priority {
		return nil
	}

	return (heap.Pop(&queue.queue).(*Execution))
}

func (queue *ExecutionQueue) Remove(execution *Execution) {
	if execution.index != -1 {
		heap.Remove(&queue.queue, execution.index)
	}
}

type PriorityQueue []*Execution

func (queue PriorityQueue) Len() int { return len(queue) }

func (queue PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest priority, not lowest
	// Then we return greatest instead of lowest here
	return queue[i].priority > queue[j].priority
}

func (queue PriorityQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
	queue[i].index = i
	queue[j].index = j
}

func (queue *PriorityQueue) Push(item any) {
	n := len(*queue)
	execution := item.(*Execution)
	execution.index = n
	*queue = append(*queue, execution)
}

func (queue *PriorityQueue) Pop() any {
	oldQueue := *queue
	size := len(oldQueue)
	execution := oldQueue[size-1]
	oldQueue[size-1] = nil
	execution.index = -1
	*queue = oldQueue[0 : size-1]

	return execution
}
