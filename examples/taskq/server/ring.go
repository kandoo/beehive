package server

import "encoding/gob"

// TaskRing is an efficient, gob-compatible ring buffer for Tasks.
// TaskRing always wastes one element.
type TaskRing struct {
	Start int    // Start points to the first written element.
	End   int    // End points to the first free element.
	Tasks []Task // Tasks stores the tasks.
	Stats Stats
}

// Stats represents the statistics of the task ring buffer.
type Stats struct {
	Deque  uint64 // Deque represents the total nubmer of dequeued tasks.
	Enque  uint64 // Enque represents the total nubmer of enqued tasks.
	Expand uint64 // Expand represents the total number of buffer expansions.
	Shrink uint64 // Shrink represents the total number of buffer shrinkings.
}

// Len returns the number of occupied positions of the ring buffer.
func (r *TaskRing) Len() int {
	if r.Start <= r.End {
		return r.End - r.Start
	}

	return r.End + r.Cap() - r.Start
}

// Cap returns the capacity of the ring buffer.
func (r *TaskRing) Cap() int {
	return len(r.Tasks)
}

// Empty returns whether the ring buffer is empty.
func (r *TaskRing) Empty() bool {
	return r.End == r.Start
}

// Full returns whether the ring buffer is full.
func (r *TaskRing) Full() bool {
	if r.Cap() == 0 {
		return true
	}

	s := r.Start - 1
	// This is faster than modulo for large slices.
	if s < 0 {
		s = r.Cap() - 1
	}
	return s == r.End
}

// Enque enqueues a task in the ring buffer.
func (r *TaskRing) Enque(t Task) {
	r.maybeExpand()

	r.Tasks[r.End] = t
	r.End++
	// This is faster than modulo for large slices.
	if r.End == r.Cap() {
		r.End = 0
	}

	r.Stats.Enque++
}

// Deque dequeues a task from the ring.
func (r *TaskRing) Deque() (t Task, ok bool) {
	if r.Empty() {
		return
	}

	ok = true
	t = r.Tasks[r.Start]

	// Reset the empty elements for better garbage collection.
	r.Tasks[r.Start] = Task{}
	r.Start++
	if r.Start == r.Cap() {
		r.Start = 0
	}

	r.maybeShrink()
	r.Stats.Deque++

	return
}

// Remove deletes a task with the given ID from the ring buffer.
// This operation is of O(n).
func (r *TaskRing) Remove(id uint64) (ok bool) {
	if r.Empty() {
		return false
	}

	var ranges [][2]int
	if r.Start < r.End {
		ranges = append(ranges, [2]int{r.Start, r.End})
	} else {
		ranges = append(ranges, [2]int{r.Start, r.Cap()})
		ranges = append(ranges, [2]int{0, r.End})
	}

	for _, rg := range ranges {
		for i := rg[0]; i < rg[1]; i++ {
			if r.Tasks[i].ID == id {
				r.remove(i)
				return true
			}
		}
	}
	return false
}

// remove deletes the task at index i from the ring buffer.
func (r *TaskRing) remove(i int) {
	if i < r.End {
		copy(r.Tasks[i:], r.Tasks[i+1:r.End])
		r.Tasks[r.End] = Task{}
	} else {
		i = r.Start - i
		tasks := make([]Task, r.Cap())
		r.CopyTo(tasks)
		copy(tasks[i:], tasks[i+1:])
		tasks[r.End] = Task{}
		r.Tasks = tasks
	}

	r.End = (r.End - 1) % r.Cap()
}

// CopyTo copies the tasks stored in this ring into tasks.
// To have a full copy, the length of tasks must be larger than r.Len().
func (r *TaskRing) CopyTo(tasks []Task) int {
	if r.Start < r.End {
		return copy(tasks, r.Tasks[r.Start:r.End])
	}

	c := r.Cap()
	n := copy(tasks, r.Tasks[r.Start:c])
	n += copy(tasks[c-r.Start:], r.Tasks[:r.End])
	return n
}

func (r *TaskRing) maybeShrink() bool {
	c, l := r.Cap(), r.Len()
	if c/4 < l {
		return false
	}

	tasks := make([]Task, c/2)
	r.CopyTo(tasks)
	r.Tasks = tasks
	r.Start = 0
	r.End = l

	r.Stats.Shrink++
	return true
}

func (r *TaskRing) maybeExpand() bool {
	if !r.Full() {
		return false
	}

	c, l := r.Cap(), r.Len()
	if c == 0 {
		c = 1
	}

	tasks := make([]Task, c*2)
	r.CopyTo(tasks)
	r.Tasks = tasks
	r.Start = 0
	r.End = l

	r.Stats.Expand++
	return true
}

func init() {
	gob.Register(TaskRing{})
}
