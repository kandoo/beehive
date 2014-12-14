// +build go1.3

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
					Timeout:   timeout,
					KeepAlive: 30 * time.Second,
				},
				MaxConnPerHost: maxConnPerHost,
			}).Dial,
			Proxy:               http.ProxyFromEnvironment,
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConnsPerHost: maxConnPerHost,
		},
		Timeout: timeout,
	}
}
