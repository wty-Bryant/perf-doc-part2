package main

import (
	"encoding/json"
	"os"
)

type WorkloadConfig struct {
	Version      int               `json:"version"`
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	Service      string            `json:"service"`
	Action       string            `json:"action"`
	ActionConfig ActionConfig      `json:"actionConfig"`
	Batch        BatchConfig       `json:"batch"`
	Warmup       WarmupConfig      `json:"warmup"`
	Measurement  MeasurementConfig `json:"measurement"`
}

type ActionConfig struct {
	// Shared
	Region    string `json:"region"`
	KeyPrefix string `json:"keyPrefix,omitempty"`
	// DDB
	TableName  string `json:"tableName,omitempty"`
	DataLength int    `json:"dataLength,omitempty"`
	// S3
	BucketName  string  `json:"bucketName,omitempty"`
	ObjectSize  int     `json:"objectSize,omitempty"`
	FilesOnDisk bool    `json:"filesOnDisk,omitempty"`
	Checksum    *string `json:"checksum,omitempty"`
}

type BatchConfig struct {
	Description         string `json:"description"`
	NumberOfActions     int    `json:"numberOfActions"`
	SequentialExecution bool   `json:"sequentialExecution"`
}

type WarmupConfig struct {
	Batches int `json:"batches"`
}

type MeasurementConfig struct {
	Batches         int  `json:"batches"`
	CollectMetrics  bool `json:"collectMetrics"`
	MetricsInterval int  `json:"metricsInterval"`
}

func loadWorkloadConfig(path string) (*WorkloadConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg WorkloadConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
