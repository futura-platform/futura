package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/stack1ng/chromedp"
)

var errChallengePageEncountered = errors.New("encountered JS challenge page")

func initializeSession(ctx context.Context, _ struct{}) error {
	sessionInitLocation, err := url.Parse(fmt.Sprintf("https://www.google.com/search?q=%d", time.Now().UnixMilli()))
	if err != nil {
		return fmt.Errorf("failed to parse session init location: %w", err)
	}

	httpClient := useHttpClient(ctx)
	var cookies []*network.Cookie
	var hasDataVed bool
	err = chromedp.Run(ctx,
		chromedp.Navigate("about:blank"),
		chromedp.Sleep(time.Second),
		chromedp.Navigate(sessionInitLocation.String()),
		chromedp.WaitReady("body"),
		chromedp.ActionFunc(func(ctx context.Context) error {
			var err error
			cookies, err = network.GetCookies().Do(ctx)
			return err
		}),
		chromedp.Evaluate(`document.querySelectorAll('[data-ved]').length > 0`, &hasDataVed),
	)
	if err != nil {
		return fmt.Errorf("failed to navigate to Google: %w", err)
	}
	if !hasDataVed {
		return fmt.Errorf("not on Google results page (possibly captcha page): %w", errChallengePageEncountered)
	}

	httpCookies := make([]*http.Cookie, len(cookies))
	for i, cookie := range cookies {
		httpCookies[i] = &http.Cookie{
			Name:    cookie.Name,
			Value:   cookie.Value,
			Domain:  cookie.Domain,
			Path:    cookie.Path,
			Expires: time.Unix(int64(cookie.Expires), 0),
		}
	}
	httpClient.Jar.SetCookies(sessionInitLocation, httpCookies)
	return nil
}
