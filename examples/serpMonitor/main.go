package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
)

func (s serpRankings) String() string {
	lines := make([]string, len(s))
	for i, r := range s {
		lines[i] = fmt.Sprintf("%d. %s - %s", i+1, r.Title, r.URL)
	}
	return strings.Join(lines, "\n")
}

func main() {
	// Prompt user for search term
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter search term to monitor: ")

	term, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("failed to read search term: %v", err)
	}

	term = strings.TrimSpace(term)
	if term == "" {
		fmt.Fprintf(os.Stderr, "Error: search term cannot be empty\n")
		os.Exit(1)
	}

	err = runSerpMonitor(context.Background(), term)
	if err != nil {
		log.Fatalf("failed to run serp monitor: %v", err)
	}
}

func runSerpMonitor(ctx context.Context, term string) error {
	ctx, cancel, err := withBrowserTab(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	_, err = futura.NewFlow[string, []serpEntry]().Execute(ctx,
		serpMonitorFlow,
		term,
		fopt.WithOnStepError(func(_ context.Context, _ string, _ []runtime.Frame, err error) bool {
			fmt.Println("error:", err)
			time.Sleep(time.Second)
			return true
		}),
		fopt.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))),
	)
	if err != nil {
		return err
	}
	return nil
}
