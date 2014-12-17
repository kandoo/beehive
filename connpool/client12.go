// +build !go1.3

package connpool

import (
	"net"
	"net/http"
	"time"
)

func newHTTPClient(maxConnPerHost int, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial: (&Dialer{
				Dialer: net.Dialer{
					Timeout: timeout,
				},
				MaxConnPerHost: maxConnPerHost,
			}).Dial,
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConnsPerHost:   maxConnPerHost,
			ResponseHeaderTimeout: timeout,
		},
	}
}
