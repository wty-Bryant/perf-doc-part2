package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: go-workload-runner <workload-file> <region>\n")
		os.Exit(1)
	}

	workloadPath := os.Args[1]
	region := os.Args[2]

	runner := NewWorkloadRunner(workloadPath, region)

	runner.Run()
}

func NewWorkloadRunner(workloadPath, region string) *WorkloadRunner {
	cfg, err := loadWorkloadConfig(workloadPath)
	if err != nil {
		log.Fatalf("failed to load workload config: %v", err)
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	r := &WorkloadRunner{
		config: cfg,
		region: region,
	}

	switch cfg.Service {
	case "s3":
		r.s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})
		if cfg.ActionConfig.ObjectSize > 0 {
			r.randomData = make([]byte, cfg.ActionConfig.ObjectSize)
			rand.Read(r.randomData)
		}
	case "dynamodb":
		r.ddbClient = dynamodb.NewFromConfig(awsCfg)
	}

	fmt.Println("Initialized WorkloadRunner:")
	fmt.Printf("  Workload: %s\n", cfg.Name)
	fmt.Printf("  Service: %s\n", cfg.Service)
	fmt.Printf("  Action: %s\n", cfg.Action)
	fmt.Printf("  Region: %s\n", region)
	fmt.Printf("  Sequential: %v\n", cfg.Batch.SequentialExecution)
	fmt.Printf("  Actions per batch: %d\n", cfg.Batch.NumberOfActions)
	return r
}

type WorkloadRunner struct {
	config               *WorkloadConfig
	region               string
	s3Client             *s3.Client
	ddbClient            *dynamodb.Client
	randomData           []byte
	randomStr            string
	operationDurationsNs []int64
	batchMu              sync.Mutex
}

func (r *WorkloadRunner) Run() {
	ctx := context.Background()

	fmt.Println("\n=== WARMUP PHASE ===")
	fmt.Printf("Executing %d warmup batches...\n", r.config.Warmup.Batches)
	for i := 0; i < r.config.Warmup.Batches; i++ {
		fmt.Printf("Warmup batch %d/%d\n", i+1, r.config.Warmup.Batches)
		r.executeBatch(ctx, false)
	}

	r.operationDurationsNs = nil
	measurementStart := time.Now()

	monitor := &ResourceMonitor{}
	if r.config.Measurement.CollectMetrics {
		monitorFn := monitor.Start(time.Duration(r.config.Measurement.MetricsInterval) * time.Millisecond)
		go monitorFn()
	}

	fmt.Println("\n=== MEASUREMENT PHASE ===")
	fmt.Printf("Executing %d measurement batches...\n", r.config.Measurement.Batches)
	for i := 0; i < r.config.Measurement.Batches; i++ {
		oldLen := len(r.operationDurationsNs)
		fmt.Printf("\nMeasurement batch %d/%d\n", i+1, r.config.Measurement.Batches)
		batchStart := time.Now()
		r.executeBatch(ctx, true)
		batchDuration := time.Since(batchStart)
		batchLen := len(r.operationDurationsNs) - oldLen
		batchDurationsNs := make([]int64, batchLen)
		copy(batchDurationsNs, r.operationDurationsNs[oldLen:])
		fmt.Printf("\n=== Batch %d Results ===\n", i)
		r.printBenchmarkResults(batchDurationsNs, int64(batchDuration))
	}

	fmt.Println("\n=== Overall Results ===\n")
	r.printBenchmarkResults(r.operationDurationsNs, int64(time.Since(measurementStart)))

	if r.config.Measurement.CollectMetrics {
		stats := monitor.Stop()
		stats.Print()
	}
}

func (r *WorkloadRunner) executeBatch(ctx context.Context, measure bool) {
	if r.config.Batch.SequentialExecution {
		r.executeSequential(ctx, measure)
	} else {
		r.executeConcurrent(ctx, measure)
	}
}

func (r *WorkloadRunner) executeSequential(ctx context.Context, measure bool) {
	n := r.config.Batch.NumberOfActions
	for i := 0; i < n; i++ {
		start := time.Now()
		if err := r.executeAction(ctx, i); err != nil {
			fmt.Fprintf(os.Stderr, "action %d failed: %v\n", i, err)
			continue
		}
		if measure {
			r.operationDurationsNs = append(r.operationDurationsNs, int64(time.Since(start)))
		}
	}
}

func (r *WorkloadRunner) executeConcurrent(ctx context.Context, measure bool) {
	n := r.config.Batch.NumberOfActions
	var wg sync.WaitGroup
	ch := make(chan int, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				idx, ok := <-ch
				if !ok {
					break
				}
				start := time.Now()
				if err := r.executeAction(ctx, idx); err != nil {
					fmt.Fprintf(os.Stderr, "action %d failed: %v\n", idx, err)
					break
				}
				if measure {
					r.batchMu.Lock()
					r.operationDurationsNs = append(r.operationDurationsNs, int64(time.Since(start)))
					r.batchMu.Unlock()
				}
			}
		}()
	}

	for i := 0; i < n; i++ {
		ch <- i
	}

	close(ch)
	wg.Wait()
}

func (r *WorkloadRunner) executeAction(ctx context.Context, index int) error {
	switch r.config.Service {
	case "s3":
		switch r.config.Action {
		case "upload":
			return r.s3Upload(ctx, index)
		case "download":
			return r.s3Download(ctx, index)
		}
	case "dynamodb":
		switch r.config.Action {
		case "putitem":
			return r.ddbPutItem(ctx, index)
		case "getitem":
			return r.ddbGetItem(ctx, index)
		}
	}
	return fmt.Errorf("unknown service/action: %s/%s", r.config.Service, r.config.Action)
}

func (r *WorkloadRunner) printBenchmarkResults(operationDurationsNs []int64, durationNs int64) {
	// get mean p50/p90/p99 and stddev of operations latency, batch total latency and throughput for both benchmarks
	count := len(operationDurationsNs)
	sec := float64(durationNs) / 1e9
	slices.Sort(operationDurationsNs)
	var singleRequestBytes int64
	if r.config.ActionConfig.ObjectSize > 0 {
		singleRequestBytes = int64(r.config.ActionConfig.ObjectSize)
	} else if r.config.ActionConfig.DataLength > 0 {
		singleRequestBytes = int64(r.config.ActionConfig.DataLength)
	}
	throughputGbps := (float64(singleRequestBytes) * 8 * float64(count) / 1e9) / sec
	min := operationDurationsNs[0]
	max := operationDurationsNs[count-1]
	p50 := operationDurationsNs[count/2]
	p90 := operationDurationsNs[int(float64(count)*0.9)]
	p99 := operationDurationsNs[int(float64(count)*0.99)]
	avg := float64(0)
	for _, s := range operationDurationsNs {
		avg += float64(s) / float64(count)
	}
	stddev := float64(0)
	for _, s := range operationDurationsNs {
		stddev += ((float64(s) - avg) * (float64(s) - avg)) / float64(count)
	}
	stddev = math.Sqrt(stddev)
	fmt.Printf("total operations: %d\n", count)
	fmt.Printf("total operations latency in ns: %d\n", durationNs)
	fmt.Printf("throughput gbps: %.2f\n", throughputGbps)
	fmt.Printf("min operation latency in ns: %d\n", min)
	fmt.Printf("max operation latency in ns: %d\n", max)
	fmt.Printf("avg operation latency in ns: %.2f\n", avg)
	fmt.Printf("p50 in ns: %d\n", p50)
	fmt.Printf("p90 in ns: %d\n", p90)
	fmt.Printf("p99 in ns: %d\n", p99)
	fmt.Printf("stddev operation latency in ns: %.2f\n", stddev)
}

func (r *WorkloadRunner) s3Upload(ctx context.Context, index int) error {
	ac := r.config.ActionConfig
	key := fmt.Sprintf("%s%d", ac.KeyPrefix, index+1)
	_, err := r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(ac.BucketName),
		Key:           aws.String(key),
		Body:          bytes.NewReader(r.randomData),
		ContentLength: aws.Int64(int64(ac.ObjectSize)),
	})
	return err
}

func (r *WorkloadRunner) s3Download(ctx context.Context, index int) error {
	ac := r.config.ActionConfig
	key := fmt.Sprintf("%s%d", ac.KeyPrefix, index+1)
	out, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ac.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	io.Copy(io.Discard, out.Body)
	out.Body.Close()
	return nil
}

func (r *WorkloadRunner) ddbPutItem(ctx context.Context, index int) error {
	ac := r.config.ActionConfig
	key := fmt.Sprintf("%s%d", ac.KeyPrefix, index+1)
	_, err := r.ddbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(ac.TableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: key},
			"data": &ddbtypes.AttributeValueMemberS{Value: r.generateRandomString(ac.DataLength)},
		},
	})
	return err
}

func (r *WorkloadRunner) ddbGetItem(ctx context.Context, index int) error {
	ac := r.config.ActionConfig
	key := fmt.Sprintf("%s%d", ac.KeyPrefix, index+1)
	_, err := r.ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(ac.TableName),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: key},
		},
	})
	return err
}

func (r *WorkloadRunner) generateRandomString(length int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[n.Int64()]
	}
	return string(b)
}
