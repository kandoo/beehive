// +build !go1.3

package beehive

import (
	"net"
	"net/http"
	"time"
)

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial:  (&net.Dialer{Timeout: timeout}).Dial,
			Proxy: http.ProxyFromEnvironment,
			ResponseHeaderTimeout: timeout,
		},
	}
}
