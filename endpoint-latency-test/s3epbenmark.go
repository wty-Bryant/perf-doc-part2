package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyauth "github.com/aws/smithy-go/auth"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/ptr"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func runS3EndpointLatency(data s3BenchmarkTestCase) {
	resolver := s3.NewDefaultEndpointResolverV2()

	// Expected endpoint for validation
	uri, _ := url.Parse(data.expectedURI)
	expectEndpoint := smithyendpoints.Endpoint{
		URI:        *uri,
		Headers:    http.Header{},
		Properties: data.expectProperties(),
	}

	const iterations = 1000
	const warmupIterations = 200

	// Warmup phase
	for i := 0; i < warmupIterations; i++ {
		_, err := resolver.ResolveEndpoint(context.Background(), data.params)
		if err != nil {
			log.Fatalf("expect no error during warmup, got %v", err)
		}
	}

	// Measurement phase
	start := time.Now()

	for i := 0; i < iterations; i++ {
		// Bind input params to endpoint params
		result, err := resolver.ResolveEndpoint(context.Background(), data.params)

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

	fmt.Printf("Warmup iterations: %d\n", warmupIterations)
	fmt.Printf("Measured iterations: %d\n", iterations)
	fmt.Printf("Total time for %d iterations: %v\n", iterations, elapsed)
	fmt.Printf("Average latency per operation: %v\n", avgLatency)
}

type s3BenchmarkTestCase struct {
	params           s3.EndpointParameters
	expectedURI      string
	expectProperties func() smithy.Properties
}

func runS3EPBenchmark() {
	cases := map[string]s3BenchmarkTestCase{
		"vanilla virtual addressing@us-west-2": {
			params: s3.EndpointParameters{
				Accelerate:     ptr.Bool(false),
				Bucket:         ptr.String("bucket-name"),
				ForcePathStyle: ptr.Bool(false),
				Region:         ptr.String("us-west-2"),
				UseDualStack:   ptr.Bool(false),
				UseFIPS:        ptr.Bool(false),
			},
			expectedURI: "https://bucket-name.s3.us-west-2.amazonaws.com",
			expectProperties: func() smithy.Properties {
				var out smithy.Properties
				smithyauth.SetAuthOptions(&out, []*smithyauth.Option{
					{
						SchemeID: "aws.auth#sigv4",
						SignerProperties: func() smithy.Properties {
							var sp smithy.Properties
							smithyhttp.SetSigV4SigningName(&sp, "s3")
							smithyhttp.SetSigV4ASigningName(&sp, "s3")

							smithyhttp.SetSigV4SigningRegion(&sp, "us-west-2")

							smithyhttp.SetDisableDoubleEncoding(&sp, true)
							return sp
						}(),
					},
				})
				return out
			},
		},
		"vanilla path style@us-west-2": {
			params: s3.EndpointParameters{
				Accelerate:     ptr.Bool(false),
				Bucket:         ptr.String("bucket-name"),
				ForcePathStyle: ptr.Bool(true),
				Region:         ptr.String("us-west-2"),
				UseDualStack:   ptr.Bool(false),
				UseFIPS:        ptr.Bool(false),
			},
			expectedURI: "https://s3.us-west-2.amazonaws.com/bucket-name",
			expectProperties: func() smithy.Properties {
				var out smithy.Properties
				smithyauth.SetAuthOptions(&out, []*smithyauth.Option{
					{
						SchemeID: "aws.auth#sigv4",
						SignerProperties: func() smithy.Properties {
							var sp smithy.Properties
							smithyhttp.SetSigV4SigningName(&sp, "s3")
							smithyhttp.SetSigV4ASigningName(&sp, "s3")

							smithyhttp.SetSigV4SigningRegion(&sp, "us-west-2")

							smithyhttp.SetDisableDoubleEncoding(&sp, true)
							return sp
						}(),
					},
				})
				return out
			},
		},
		"Data Plane with short zone name": {
			params: s3.EndpointParameters{
				Region:                      ptr.String("us-east-1"),
				Bucket:                      ptr.String("mybucket--abcd-ab1--x-s3"),
				UseFIPS:                     ptr.Bool(false),
				UseDualStack:                ptr.Bool(false),
				Accelerate:                  ptr.Bool(false),
				UseS3ExpressControlEndpoint: ptr.Bool(false),
			},
			expectedURI: "https://mybucket--abcd-ab1--x-s3.s3express-abcd-ab1.us-east-1.amazonaws.com",
			expectProperties: func() smithy.Properties {
				var out smithy.Properties
				smithyauth.SetAuthOptions(&out, []*smithyauth.Option{
					{
						SchemeID: "sigv4-s3express",
						SignerProperties: func() smithy.Properties {
							var sp smithy.Properties
							smithyhttp.SetSigV4SigningName(&sp, "s3express")
							smithyhttp.SetSigV4ASigningName(&sp, "s3express")

							smithyhttp.SetSigV4SigningRegion(&sp, "us-east-1")

							smithyhttp.SetDisableDoubleEncoding(&sp, true)
							return sp
						}(),
					},
				})
				out.Set("backend", "S3Express")
				return out
			},
		},
		"vanilla access point arn@us-west-2": {
			params: s3.EndpointParameters{
				Accelerate:     ptr.Bool(false),
				Bucket:         ptr.String("arn:aws:s3:us-west-2:123456789012:accesspoint:myendpoint"),
				ForcePathStyle: ptr.Bool(false),
				Region:         ptr.String("us-west-2"),
				UseDualStack:   ptr.Bool(false),
				UseFIPS:        ptr.Bool(false),
			},
			expectedURI: "https://myendpoint-123456789012.s3-accesspoint.us-west-2.amazonaws.com",
			expectProperties: func() smithy.Properties {
				var out smithy.Properties
				smithyauth.SetAuthOptions(&out, []*smithyauth.Option{
					{
						SchemeID: "aws.auth#sigv4",
						SignerProperties: func() smithy.Properties {
							var sp smithy.Properties
							smithyhttp.SetSigV4SigningName(&sp, "s3")
							smithyhttp.SetSigV4ASigningName(&sp, "s3")

							smithyhttp.SetSigV4SigningRegion(&sp, "us-west-2")

							smithyhttp.SetDisableDoubleEncoding(&sp, true)
							return sp
						}(),
					},
				})
				return out
			},
		},
		"S3 outposts vanilla test": {
			params: s3.EndpointParameters{
				Region:       ptr.String("us-west-2"),
				UseFIPS:      ptr.Bool(false),
				UseDualStack: ptr.Bool(false),
				Accelerate:   ptr.Bool(false),
				Bucket:       ptr.String("arn:aws:s3-outposts:us-west-2:123456789012:outpost/op-01234567890123456/accesspoint/reports"),
			},
			expectedURI: "https://reports-123456789012.op-01234567890123456.s3-outposts.us-west-2.amazonaws.com",
			expectProperties: func() smithy.Properties {
				var out smithy.Properties
				smithyauth.SetAuthOptions(&out, []*smithyauth.Option{
					{
						SchemeID: "aws.auth#sigv4a",
						SignerProperties: func() smithy.Properties {
							var sp smithy.Properties
							smithyhttp.SetSigV4SigningName(&sp, "s3-outposts")
							smithyhttp.SetSigV4ASigningName(&sp, "s3-outposts")

							smithyhttp.SetSigV4ASigningRegions(&sp, []string{"*"})

							smithyhttp.SetDisableDoubleEncoding(&sp, true)
							return sp
						}(),
					},
					{
						SchemeID: "aws.auth#sigv4",
						SignerProperties: func() smithy.Properties {
							var sp smithy.Properties
							smithyhttp.SetSigV4SigningName(&sp, "s3-outposts")
							smithyhttp.SetSigV4ASigningName(&sp, "s3-outposts")

							smithyhttp.SetSigV4SigningRegion(&sp, "us-west-2")

							smithyhttp.SetDisableDoubleEncoding(&sp, true)
							return sp
						}(),
					},
				})
				return out
			},
		},
	}
	for name, tc := range cases {
		fmt.Printf("Running test case: %s\n", name)
		runS3EndpointLatency(tc)
		fmt.Printf("===============\n")
	}
}
