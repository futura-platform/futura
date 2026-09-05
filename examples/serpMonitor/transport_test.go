package main

import (
	"net/http"
	"testing"
)

func TestTransportDialsTheHostDirectly(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://www.google.com/", nil)
	if serpMonitorHTTPTransport.Proxy != nil {
		proxy, err := serpMonitorHTTPTransport.Proxy(req)
		if err != nil {
			t.Fatal(err)
		}
		if proxy != nil {
			t.Fatalf("the example proxies through %s, which nobody running it has", proxy)
		}
	}
	if tlsConfig := serpMonitorHTTPTransport.TLSClientConfig; tlsConfig != nil && tlsConfig.InsecureSkipVerify {
		t.Fatal("the example skips certificate verification")
	}
}
