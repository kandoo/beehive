package beehive

import "net/http"

type httpClient struct {
	dataClient *http.Client
	cmdClient  *http.Client
	raftClient *http.Client
}

func newSimpleHttpClient(client *http.Client) httpClient {
	return httpClient{
		dataClient: client,
		cmdClient:  client,
		raftClient: client,
	}
}
