package utils

import "testing"

func TestJob(t *testing.T) {
	dsp := NewDispatcher(10, 10)
	dsp.Run()
}
