package timewheel

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeWheel_AddTask(t *testing.T) {
	tw := New(time.Second, 3600)
	tw.Start()
	defer tw.Stop()

	durations := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}

	for _, d := range durations {
		t.Run("", func(t *testing.T) {
			exitC := make(chan time.Time)
			start := time.Now()
			tw.AddTask(d, 1, fmt.Sprintf("key%d", d), fmt.Sprintf("val%d", d), func(p interface{}) {
				exitC <- time.Now()
			})

			got := (<-exitC).Truncate(time.Second)
			min := start.Add(d).Truncate(time.Second)

			err := 1 * time.Second
			if got.Before(min) || got.After(min.Add(err)) {
				t.Errorf("Timer(%s) expiration: want [%s,%s], got %s", d, min, min.Add(err), got)
			}
		})
	}
}

type S struct {
	intervals []time.Duration
	current   int
}

func (s *S) Next() time.Duration {
	var d time.Duration
	if len(s.intervals) < 1 {
		return d
	}
	if s.current >= len(s.intervals) {
		return d
	}
	d = s.intervals[s.current]
	s.current += 1
	return d
}

func TestTimeWheel_SchedulerTask(t *testing.T) {
	s := S{
		intervals: []time.Duration{
			5 * time.Second,
			55 * time.Second,
			30 * time.Second,
		}}

	tw := New(time.Second, 3600)
	tw.Start()
	defer tw.Stop()

	exitC := make(chan time.Time, len(s.intervals))
	start := time.Now()
	tw.SchedulerTask(&s, "schedulerKey", "schedulerVal", func(p interface{}) {
		exitC <- time.Now()
	})

	accum := time.Duration(0)
	for _, d := range s.intervals {
		got := (<-exitC).Truncate(time.Second)
		accum += d
		min := start.Add(accum).Truncate(time.Second)

		err := 1 * time.Second
		if got.Before(min) || got.After(min.Add(err)) {
			t.Errorf("Timer(%s) expiration: want [%s,%s], got %s", accum, min, min.Add(err), got)
		}
	}

}

func TestTimeWheel_RunTask(t *testing.T) {
	tw := New(time.Millisecond, 3000)
	tw.Start()
	defer tw.Stop()

	exitC := make(chan time.Time)
	start := time.Now()
	tw.RunTask("runtaskVal", func(p interface{}) {
		exitC <- time.Now()
	})

	got := (<-exitC).Truncate(time.Millisecond)
	min := start.Truncate(time.Millisecond)
	err := 5 * time.Millisecond
	if got.After(min.Add(err)) {
		t.Errorf("Timer(%s) expiration: want [%s,%s], got %s", time.Duration(0), min, min.Add(err), got)
	}
}
