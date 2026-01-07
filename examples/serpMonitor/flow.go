package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/seal"
)

func serpMonitorFlow(b futura.FlowBuilder, term string) ([]serpEntry, error) {
	sessionValid := futura.State(b, true)

	if !sessionValid.V() {
		err := futura.Effect(b, initializeSession, struct{}{})
		if err != nil {
			return nil, err
		}
		sessionValid.Set(true)
	}

	var lastRankings seal.Sealed[serpRankings]
	for i := 0; ; func() { i++; time.Sleep(time.Second) }() {
		b := b.WithKey(i)
		newRankings, err := futura.Step(b, fetchRankings, fetchRankingsParams{
			term:         term,
			sessionValid: sessionValid.V(),
		})
		if err != nil {
			if errors.Is(err, errChallengePageEncountered) {
				sessionValid.Set(false)
			}
			return nil, err
		}

		// diff the new rankings with the last rankings, and print and update if something changed
		if lastRankings != newRankings {
			fmt.Printf("Rankings changed for '%s'!\n%s\n", term, newRankings.V().String())
			lastRankings = newRankings
		}
	}
}
