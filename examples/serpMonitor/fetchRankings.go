package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/futura-platform/futura/ftype"
)

type serpEntry struct {
	Title string
	URL   string
}

type serpRankings []serpEntry

type fetchRankingsParams struct {
	term         string
	sessionValid bool
}

func fetchRankings(ctx context.Context, p fetchRankingsParams) (ftype.Sealed[serpRankings], error) {
	httpClient := useHttpClient(ctx)
	request, err := http.NewRequest("GET", "https://www.google.com/search?q="+p.term, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// parse the response body with goquery
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response body: %w", err)
	}

	// detect if we received the JS challenge page, if so then retry initialization
	if doc.Find("script").FilterFunction(func(i int, s *goquery.Selection) bool {
		return strings.Contains(s.Text(), `SG_SS=`)
	}).Length() > 0 {
		return nil, errChallengePageEncountered
	}

	// find the top-level search result anchors
	resultAnchors := doc.Find("a[data-ved]").FilterFunction(func(i int, s *goquery.Selection) bool {
		h3Children := s.Children().Find("h3").Length()
		return h3Children > 0
	})

	// if we can't find any, assume we were hit with a challenge page
	if resultAnchors.Length() == 0 {
		return nil, errChallengePageEncountered
	}

	topLevelSearchResults := make(serpRankings, resultAnchors.Length())
	for i, a := range resultAnchors.EachIter() {
		href, exists := a.Attr("href")
		if !exists {
			return nil, fmt.Errorf("search result anchor does not have href attribute")
		}
		u, err := url.Parse(href)
		if err != nil {
			return nil, fmt.Errorf("failed to parse search result URL: %w", err)
		}
		externalUrl, err := url.Parse(u.Query().Get("q"))
		if err != nil {
			return nil, fmt.Errorf("failed to parse external URL: %w", err)
		}
		topLevelSearchResults[i] = serpEntry{
			Title: a.Find("h3").First().Text(),
			URL:   externalUrl.String(),
		}
	}

	return ftype.Seal(topLevelSearchResults), nil
}
