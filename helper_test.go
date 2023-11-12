package main

import "testing"

type addrParseConvTest struct {
	grpcAddr string
	host     string
	httpPort string
	err      error
}

var addrParseConvTests = []addrParseConvTest{
	{grpcAddr: "localhost:3000", host: "localhost", httpPort: "2000", err: nil},
}

func TestGetHttpAddrFromGrpcAddr(t *testing.T) {
	for _, test := range addrParseConvTests {
		host, httpPort, err := getHttpAddrFromGrpcAddr(test.grpcAddr)
		if host != test.host || httpPort != test.httpPort || err != test.err {
			t.Errorf(
				"grpcAddr='%s'\nExpected - host=%s, port=%s, err=%v\nGot - host=%s, port=%s, err=%v",
				test.grpcAddr, test.host, test.httpPort, test.err,
				host, httpPort, err)
		}
	}
}
