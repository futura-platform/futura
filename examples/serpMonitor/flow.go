package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/seal"
)

func serpMonitorFlow(b futura.FlowBuilder, term string) ([]serpEntry, error) {
	b = withHttpClient(b)
	sessionValid := futura.State(b, true)

	// the session is re-initialized every time it goes invalid, not just the first time: When gives the
	// branch a fresh identity on each reopening, so the effect is not served from its memo
	err := futura.When(b, !sessionValid.V(), func(b futura.FlowBuilder) error {
		if err := futura.Effect(b, initializeSession, struct{}{}); err != nil {
			return err
		}
		sessionValid.Set(true)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var lastRankings seal.Sealed[serpRankings]
	for i := 0; ; i++ {
		b := b.WithKey(strconv.Itoa(i))
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

		// printing is a side effect like any other, so it is a step: on replay it is memoized and not
		// printed again
		if lastRankings != newRankings {
			if err := futura.Effect(b, announce, announcement{term, newRankings}); err != nil {
				return nil, err
			}
			lastRankings = newRankings
		}

		// so is waiting
		if err := futura.Effect(b, sleep, time.Second); err != nil {
			return nil, err
		}
	}
}

type announcement struct {
	term     string
	rankings seal.Sealed[serpRankings]
}

func announce(_ context.Context, a announcement) error {
	fmt.Printf("Rankings changed for '%s'!\n%s\n", a.term, a.rankings.V().String())
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
