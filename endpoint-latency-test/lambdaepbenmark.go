package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/smithy-go"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/ptr"
)

func benchmarkLambdaEPLatency(region, uristr string, useFIPs, useDualStack bool) {
	// Expected endpoint for validation
	resolver := lambda.NewDefaultEndpointResolverV2()
	uri, _ := url.Parse(uristr)
	expectEndpoint := smithyendpoints.Endpoint{
		URI:        *uri,
		Headers:    http.Header{},
		Properties: smithy.Properties{},
	}

	const iterations = 1000
	const warmupIterations = 200

	// Warmup phase
	for i := 0; i < warmupIterations; i++ {
		var params = lambda.EndpointParameters{
			Region:       ptr.String(region),
			UseFIPS:      ptr.Bool(useFIPs),
			UseDualStack: ptr.Bool(useDualStack),
		}

		_, err := resolver.ResolveEndpoint(context.Background(), params)
		if err != nil {
			log.Fatalf("expect no error during warmup, got %v", err)
		}
	}

	// Measurement phase
	start := time.Now()

	for i := 0; i < iterations; i++ {
		// Bind input params to endpoint params
		var params = lambda.EndpointParameters{
			Region:       ptr.String(region),
			UseFIPS:      ptr.Bool(useFIPs),
			UseDualStack: ptr.Bool(useDualStack),
		}

		result, err := resolver.ResolveEndpoint(context.Background(), params)

		if err != nil {
			log.Fatalf("expect no error, got %v", err)
		}

		// Validate on first iteration
		if i == 0 {
			if e, a := expectEndpoint.URI, result.URI; e != a {
				log.Fatalf("expect %v URI, got %v", e, a)
			}
			if !reflect.DeepEqual(expectEndpoint.Headers, result.Headers) {
				log.Fatalf("expect headers to match\n%v != %v", expectEndpoint.Headers, result.Headers)
			}
			if !reflect.DeepEqual(expectEndpoint.Properties, result.Properties) {
				log.Fatalf("expect properties to match\n%v != %v", expectEndpoint.Properties, result.Properties)
			}
		}
	}

	elapsed := time.Since(start)
	avgLatency := elapsed / iterations

	fmt.Printf("Total time after %d warmed up iterations: %v\n", warmupIterations, elapsed)
	fmt.Printf("Average latency per operation after warmup (param binding + ResolveEndpoint): %v\n", avgLatency)
}

func runLambdaEPBenchmark() {
	cases := map[string]struct {
		region       string
		uristr       string
		useFIPs      bool
		useDualStack bool
	}{
		"For region us-gov-east-1 with FIPS enabled and DualStack enabled": {
			region:       "us-gov-east-1",
			uristr:       "https://lambda-fips.us-gov-east-1.api.aws",
			useFIPs:      true,
			useDualStack: true,
		},
		"For region us-east-1 with FIPS disabled and DualStack disabled": {
			region: "us-east-1",
			uristr: "https://lambda.us-east-1.amazonaws.com",
		},
	}

	for name, tc := range cases {
		fmt.Printf("Running test case: %s\n", name)
		benchmarkLambdaEPLatency(tc.region, tc.uristr, tc.useFIPs, tc.useDualStack)
		fmt.Printf("===============\n")
	}
}
