package service

/*
 * Copyright 2020-2023 Aldelo, LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"encoding/json"
	"fmt"
	util "github.com/aldelo/common"
	"log"
	"time"

	"github.com/aldelo/common/wrapper/redis"
)

// RedisServiceRegistry is the service registry that registers gRPC service instances into Redis
type RedisServiceRegistry struct {
	client          *redis.Redis
	serviceName     string
	instanceID      string // ULID
	ip              string
	port            uint
	heartbeatTicker *time.Ticker
	stopChan        chan struct{}
	isRunning       bool
}

// RedisInstanceInfo is the instance info stored in Redis
type RedisInstanceInfo struct {
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	LastUpdate int64  `json:"lastUpdate"`
}

// NewRedisServiceRegistry creates a Redis service registry
func NewRedisServiceRegistry(cfg *redisData, serviceName string, ip string, port uint) (*RedisServiceRegistry, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if len(cfg.WriteEndpoint) == 0 {
		return nil, fmt.Errorf("redis write endpoints not configured")
	}

	if len(cfg.ReadEndpoint) == 0 {
		cfg.ReadEndpoint = cfg.WriteEndpoint
	}

	// create the Redis client
	writerEndpoint := cfg.WriteEndpoint
	readerEndpoint := cfg.ReadEndpoint

	redisClient := &redis.Redis{
		AwsRedisWriterEndpoint: writerEndpoint,
		AwsRedisReaderEndpoint: readerEndpoint,
	}

	// connect to Redis
	if err := redisClient.Connect(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	// generate the instance ULID
	instanceID := util.NewULID()

	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 30 // default 30 seconds
	}

	return &RedisServiceRegistry{
		client:          redisClient,
		serviceName:     serviceName,
		instanceID:      instanceID,
		ip:              ip,
		port:            port,
		heartbeatTicker: time.NewTicker(time.Duration(heartbeatInterval) * time.Second),
		stopChan:        make(chan struct{}),
		isRunning:       false,
	}, nil
}

// Start begins heartbeat registration
func (r *RedisServiceRegistry) Start() error {
	if r == nil {
		return nil
	}

	if r.isRunning {
		return fmt.Errorf("registry already running")
	}

	// register once immediately
	if err := r.register(); err != nil {
		return fmt.Errorf("initial registration failed: %w", err)
	}

	// start the heartbeat goroutine
	r.isRunning = true
	go r.heartbeatLoop()

	log.Printf("[Redis Discovery] Service '%s' registered with instance ID '%s' at %s:%d",
		r.serviceName, r.instanceID, r.ip, r.port)

	return nil
}

// Stop stops the heartbeat and deregisters the instance
func (r *RedisServiceRegistry) Stop() error {
	if r == nil {
		return nil
	}

	if !r.isRunning {
		return nil
	}

	// stop the heartbeat
	close(r.stopChan)
	r.heartbeatTicker.Stop()
	r.isRunning = false

	// delete instance info from Redis
	if err := r.deregister(); err != nil {
		log.Printf("[Redis Discovery] Failed to deregister instance %s: %v", r.instanceID, err)
		return err
	}

	log.Printf("[Redis Discovery] Service '%s' instance '%s' deregistered", r.serviceName, r.instanceID)

	return nil
}

// heartbeatLoop is the heartbeat loop
func (r *RedisServiceRegistry) heartbeatLoop() {
	for {
		select {
		case <-r.heartbeatTicker.C:
			if err := r.register(); err != nil {
				log.Printf("[Redis Discovery] Heartbeat registration failed: %v", err)
			}
		case <-r.stopChan:
			return
		}
	}
}

// register registers or updates instance info in Redis
func (r *RedisServiceRegistry) register() error {
	key := r.getRedisKey()

	instanceInfo := &RedisInstanceInfo{
		IP:         r.ip,
		Port:       r.port,
		LastUpdate: time.Now().Unix(),
	}

	data, err := json.Marshal(instanceInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal instance info: %w", err)
	}

	// use HASH.HSet to store instance info
	if err := r.client.HASH.HSet(key, r.instanceID, string(data)); err != nil {
		return fmt.Errorf("failed to set instance info: %w", err)
	}

	return nil
}

// deregister removes the instance from Redis
func (r *RedisServiceRegistry) deregister() error {
	key := r.getRedisKey()

	// use HASH.HDel to delete instance info
	if _, err := r.client.HASH.HDel(key, r.instanceID); err != nil {
		return fmt.Errorf("failed to delete instance info: %w", err)
	}

	return nil
}

// getRedisKey returns the Redis key
func (r *RedisServiceRegistry) getRedisKey() string {
	return fmt.Sprintf("grpc:services:%s", r.serviceName)
}

// GetInstanceID returns the instance ID
func (r *RedisServiceRegistry) GetInstanceID() string {
	if r == nil {
		return ""
	}
	return r.instanceID
}

// GetServiceName returns the service name
func (r *RedisServiceRegistry) GetServiceName() string {
	if r == nil {
		return ""
	}
	return r.serviceName
}
