package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/cookiejar"

	"github.com/futura-platform/futura"
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
	nil,
)

var serpMonitorHTTPTransport = &http.Transport{
	// set HTTPS_PROXY to route through a local MITM for debugging
	Proxy: http.ProxyFromEnvironment,
}

// withHttpClient provides the durable cookie jar resolver to the flow.
// Call useHttpClient(ctx) inside steps/effects to retrieve a client whose cookies are durable.
func withHttpClient(b futura.FlowBuilder) futura.FlowBuilder {
	return serpMonitorCookieJarHandle.Provide(b)
}

func useHttpClient(ctx context.Context) *http.Client {
	return &http.Client{
		Transport: serpMonitorHTTPTransport,
		Jar:       serpMonitorCookieJarHandle.Use(ctx),
	}
}
