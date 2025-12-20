package main

import (
	"context"
	"net/http"
)

type httpClientCtxKey int

const httpClientCtxKeyVal httpClientCtxKey = iota

func withHttpClient(ctx context.Context, httpClient *http.Client) context.Context {
	return context.WithValue(ctx, httpClientCtxKeyVal, httpClient)
}

func useHttpClient(ctx context.Context) *http.Client {
	return ctx.Value(httpClientCtxKeyVal).(*http.Client)
}
