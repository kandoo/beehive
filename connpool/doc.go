/*
connpool is a wrapper around http.Client that puts a cap on the number of open
connections towards a remote hosts.

To create a new http client, with maximum 100 parallel connections per remote
host and a connection/request timeout of 10sec:

	client := connpool.NewHTTPClient(100, 10 * time.Second)

You can also directly use connpool.Dialer:

	http.Client{
		Transport: &http.Transport{
			Dial: (&Dialer{
				Dialer:         net.Dialer{Timeout: 10 * time.Second},
				MaxConnPerHost: 100,
			}).Dial,
			Proxy:                 http.ProxyFromEnvironment,
		},
	}

*/
package connpool
