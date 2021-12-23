package timewheel

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"
)

// time wheel struct
type TimeWheel struct {
	interval       time.Duration
	ticker         *time.Ticker
	slots          []*list.List
	currentPos     int
	slotNum        int
	addTaskChannel chan *task
	runTaskChannel chan *task
	stopChannel    chan bool
	taskRecord     *sync.Map
}

// Job callback function
type Job func(interface{})

// Scheduler determines the execution plan of a task.
type Scheduler interface {
	// Next returns the next execution duration.
	// It will return a zero time.Duration if no next duration is scheduled.
	Next() time.Duration
}

// task struct
type task struct {
	s        Scheduler
	interval time.Duration
	times    int //-1:no limit >=1:run times
	circle   int
	pos      int
	key      interface{}
	data     interface{}
	job      Job
}

// New create a empty time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:       interval,
		slots:          make([]*list.List, slotNum),
		currentPos:     0,
		slotNum:        slotNum,
		addTaskChannel: make(chan *task),
		runTaskChannel: make(chan *task),
		stopChannel:    make(chan bool),
		taskRecord:     &sync.Map{},
	}

	tw.init()

	return tw
}

// Start start the time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stop the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(task)
		case task := <-tw.runTaskChannel:
			go task.job(task.data)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

// RunTask immediately run new task to time wheel
func (tw *TimeWheel) RunTask(data interface{}, job Job) error {
	if job == nil {
		return errors.New("illegal task params")
	}

	tw.runTaskChannel <- &task{data: data, job: job}

	return nil
}

// SchedulerTask scheduler new task to time wheel
func (tw *TimeWheel) SchedulerTask(s Scheduler, key, data interface{}, job Job) error {
	if s == nil || key == nil || job == nil {
		return errors.New("illegal task params")
	}

	taskData := &task{s: s, times: -1, key: key, data: data, job: job}
	_, loaded := tw.taskRecord.LoadOrStore(key, taskData)
	if loaded {
		return fmt.Errorf("duplicate task key=%v", key)
	}

	tw.addTaskChannel <- taskData

	return nil
}

// AddTask add new task to the time wheel
func (tw *TimeWheel) AddTask(interval time.Duration, times int, key, data interface{}, job Job) error {
	if interval <= 0 || key == nil || job == nil || times < -1 || times == 0 {
		return errors.New("illegal task params")
	}

	taskData := &task{interval: interval, times: times, key: key, data: data, job: job}
	_, loaded := tw.taskRecord.LoadOrStore(key, taskData)
	if loaded {
		return fmt.Errorf("duplicate task key=%v", key)
	}

	tw.addTaskChannel <- taskData
	return nil
}

// RemoveTask remove the task from time wheel
func (tw *TimeWheel) RemoveTask(key interface{}) error {
	if key == nil {
		return nil
	}

	value, loaded := tw.taskRecord.LoadAndDelete(key)
	if !loaded {
		return fmt.Errorf("task not exists, please check you task key=%v", key)
	} else {
		// lazy remove task
		task := value.(*task)
		task.times = 0
	}
	return nil
}

// UpdateTask update task times and data
func (tw *TimeWheel) UpdateTask(key interface{}, interval time.Duration, data interface{}) error {
	if key == nil {
		return errors.New("illegal key, please try again")
	}

	value, ok := tw.taskRecord.Load(key)
	if !ok {
		return fmt.Errorf("task not exists, please check you task key=%v", key)
	}

	task := value.(*task)
	if task.times <= 1 {
		return fmt.Errorf("task only trigger once, cannot update task key=%v", key)
	}
	task.data = data
	task.interval = interval
	return nil
}

// time wheel initialize
func (tw *TimeWheel) init() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

//
func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	tw.scanAddRunTask(l)
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
}

// add task
func (tw *TimeWheel) addTask(task *task) {
	if task.times == 0 {
		return
	}

	if task.times == -1 && task.s != nil {
		task.interval = task.s.Next()
		if task.interval == 0 {
			tw.RemoveTask(task.key)
			return
		}
	}

	pos, circle := tw.getPositionAndCircle(task.interval)
	task.circle = circle
	task.pos = pos

	tw.slots[pos].PushBack(task)
}

// scan task list and run the task
func (tw *TimeWheel) scanAddRunTask(l *list.List) {

	if l == nil {
		return
	}

	for item := l.Front(); item != nil; {
		task := item.Value.(*task)

		if task.times == 0 {
			next := item.Next()
			l.Remove(item)
			item = next
			continue
		}

		if task.circle > 0 {
			task.circle--
			item = item.Next()
			continue
		}

		go task.job(task.data)
		next := item.Next()
		l.Remove(item)
		item = next

		if task.times == 1 {
			task.times = 0
			tw.taskRecord.Delete(task.key)
		} else {
			if task.times > 0 {
				task.times--
			}
			tw.addTask(task)
		}
	}
}

// get the task position
func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum
	return
}
