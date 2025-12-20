package main

import (
	"testing"
)

// this is just a stub to make it easy to initiate a debugger session
func TestMain(t *testing.T) {
	err := runSerpMonitor(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
}
