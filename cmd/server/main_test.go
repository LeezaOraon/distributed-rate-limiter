package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseKeyBy(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "9.9.9.9:1234"
	r.Header.Set("X-API-Key", "abc")

	f, err := parseKeyBy("ip")
	if err != nil || f(r) != "ip:9.9.9.9" {
		t.Fatalf(`"ip": got (%v), err %v`, f(r), err)
	}
	f, err = parseKeyBy("header:X-API-Key")
	if err != nil || f(r) != "hdr:X-API-Key:abc" {
		t.Fatalf(`"header:X-API-Key": got (%v), err %v`, f(r), err)
	}
	for _, bad := range []string{"", "header:", "cookie", "IP"} {
		if _, err := parseKeyBy(bad); err == nil {
			t.Errorf("parseKeyBy(%q): want error", bad)
		}
	}
}
