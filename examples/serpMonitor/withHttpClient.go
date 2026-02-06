package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/privateencoding"
)

const serpMonitorCookieJarKey = "serpMonitorCookieJar"

var serpMonitorCookieJarHandle = futura.NewDurableHandle[cookiejar.Jar](
	serpMonitorCookieJarKey,
	func() *cookiejar.Jar {
		jar, err := cookiejar.New(nil)
		if err != nil {
			panic(fmt.Errorf("failed to create cookie jar: %w", err))
		}
		return jar
	},
	func(data []byte) (*cookiejar.Jar, error) {
		decoder := privateencoding.NewDecoder[*cookiejar.Jar](bytes.NewReader(data))
		return decoder.Decode()
	},
	func(jar *cookiejar.Jar) ([]byte, error) {
		var buf bytes.Buffer
		encoder := privateencoding.NewEncoder[*cookiejar.Jar](&buf)
		if err := encoder.Encode(jar); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	},
)

var serpMonitorHTTPTransport = &http.Transport{
	// proxying through a local MITM for debugging
	Proxy: http.ProxyURL(&url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:8888",
	}),
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
	},
}

// withHttpClient provides the durable cookie jar resolver to the flow.
// Call useHttpClient(ctx) inside steps/effects to retrieve a client and persist func.
func withHttpClient() ftype.FlowLoopOption {
	return serpMonitorCookieJarHandle.Provide()
}

func useHttpClient(ctx context.Context) (client *http.Client, persistCookies func() bool) {
	b, ok := ctx.(futura.FlowBuilder)
	if !ok {
		panic("useHttpClient must be called from within a futura flow")
	}

	jar, persist := serpMonitorCookieJarHandle.Use(b)
	return &http.Client{
		Transport: serpMonitorHTTPTransport,
		Jar:       jar,
	}, persist
}
