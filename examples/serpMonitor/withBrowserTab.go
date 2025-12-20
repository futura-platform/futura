package main

import (
	"context"

	"github.com/stack1ng/chromedp"
)

func withBrowserTab(ctx context.Context) (context.Context, func(), error) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-setuid-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("no-zygote", true),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("ignore-ssl-errors", true),
		chromedp.Flag("ignore-gpu-blocklist", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-features", "PrivacySandboxSettings4"),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("headless", false),
	)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(ctx, opts...)

	tabCtx, cancelTab := chromedp.NewContext(allocCtx)

	return tabCtx, func() {
			cancelTab()
			cancelAlloc()
		},
		// allocate the browser tab
		chromedp.Run(tabCtx, chromedp.ActionFunc(func(ctx context.Context) error { return nil }))
}
