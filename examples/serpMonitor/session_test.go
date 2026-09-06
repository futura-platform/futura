package main

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"testing"
	"time"

	"github.com/chromedp/cdproto/network"
)

func TestSessionCookies(t *testing.T) {
	location, err := url.Parse("https://www.google.com/search")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a session cookie is kept by the jar", func(t *testing.T) {
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		jar.SetCookies(location, []*http.Cookie{httpCookie(&network.Cookie{
			Name:    "NID",
			Value:   "session",
			Domain:  ".google.com",
			Path:    "/",
			Expires: -1,
			Session: true,
		})})
		cookies := jar.Cookies(location)
		if len(cookies) != 1 || cookies[0].Name != "NID" {
			t.Fatalf("the session cookie was dropped, jar holds %v", cookies)
		}
	})

	t.Run("a future expiry is kept as is", func(t *testing.T) {
		expires := time.Now().Add(time.Hour).Truncate(time.Second)
		cookie := httpCookie(&network.Cookie{
			Name:    "SID",
			Value:   "persistent",
			Domain:  ".google.com",
			Path:    "/",
			Expires: float64(expires.Unix()),
		})
		if !cookie.Expires.Equal(expires) {
			t.Fatalf("expires %v, want %v", cookie.Expires, expires)
		}
	})
}
