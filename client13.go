// +build go1.3

package beehive

import (
	"net"
	"net/http"
	"time"
)

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 30 * time.Second,
			}).Dial,
			Proxy:               http.ProxyFromEnvironment,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Timeout: timeout,
	}
}
