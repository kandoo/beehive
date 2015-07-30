process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

require('http2').get('https://localhost:50051/', function(response) {
		  response.pipe(process.stdout);
});

