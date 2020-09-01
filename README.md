# timewheel
golang implement for time wheel.



## example

```
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/yifeng01/timewheel"
)

type Scheduler struct {
	intervals []time.Duration
	current   int
}

func (s *Scheduler) Next() time.Duration {
	var d time.Duration
	if len(s.intervals) < 1 {
		return d
	}
	if s.current >= len(s.intervals) {
		return s.intervals[len(s.intervals)-1]
	}
	d = s.intervals[s.current]
	s.current += 1
	return d
}

func main() {
	log.Println("start...")

	s := Scheduler{
		intervals: []time.Duration{
			5 * time.Second,
			55 * time.Second,
			30 * time.Second,
		}}

	tw := timewheel.New(time.Second, 3600)
	tw.Start()
	defer tw.Stop()

	tw.SchedulerTask(&s, "22231028", "22231028val", func(para interface{}) {
		str := para.(string)
		log.Println("schedulerTask", str)
	})

	tw.AddTask(2*time.Second, -1, "22231029", "22231029val", func(para interface{}) {
		str := para.(string)
		log.Println("addTask", str)
	})

	tw.AddTask(time.Second, 1, "22231030", "22231030val", func(para interface{}) {
		str := para.(string)
		log.Println("addTask: once", str)
	})

	uid := 22231031
	for i := 0; i < 100 {
  	str := fmt.Sprintf("i=%d,%dval", i, uid+i)
		tw.RunTask(str, func(para interface{}) {
			str := para.(string)
			log.Println("runTask: once", str)
		})
	}

	select {}
  }
```
