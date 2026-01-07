package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
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
	jar, err := cookiejar.New(nil)
	if err != nil {
		return fmt.Errorf("failed to create cookie jar: %w", err)
	}
	httpClient := http.Client{
		Transport: &http.Transport{
			// proxying through a local MITM for debugging
			Proxy: http.ProxyURL(&url.URL{
				Scheme: "http",
				Host:   "127.0.0.1:8888",
			}),
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Jar: jar,
	}
	ctx, cancel, err := withBrowserTab(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	ctx = withHttpClient(ctx, &httpClient)

	_, err = futura.Flow(ctx,
		serpMonitorFlow,
		term,
		ftype.WithOnStepError(func(err error) bool {
			fmt.Println("error:", err)
			time.Sleep(time.Second)
			return true
		}),
		ftype.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))),
	)
	if err != nil {
		return err
	}
	return nil
}
