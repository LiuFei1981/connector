package client

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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aldelo/common/wrapper/redis"
)

// RedisServiceDiscovery is the client-side Redis service discovery
type RedisServiceDiscovery struct {
	client        *redis.Redis
	serviceName   string
	instanceTTL   int64 // instance TTL (seconds)
	instances     []string
	instancesLock sync.RWMutex
	currentIndex  uint32
}

// RedisInstanceInfo is the instance info stored in Redis
type RedisInstanceInfo struct {
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	LastUpdate int64  `json:"lastUpdate"`
}

// NewRedisServiceDiscovery creates a Redis service-discovery client
func NewRedisServiceDiscovery(writeEndpoint, readEndpoint string, password string, db int, serviceName string, instanceTTL uint) (*RedisServiceDiscovery, error) {
	if len(writeEndpoint) == 0 {
		return nil, fmt.Errorf("redis endpoints not configured")
	}

	if len(readEndpoint) == 0 {
		readEndpoint = writeEndpoint
	}

	writerEndpoint := writeEndpoint
	readerEndpoint := readEndpoint

	redisClient := &redis.Redis{
		AwsRedisWriterEndpoint: writerEndpoint,
		AwsRedisReaderEndpoint: readerEndpoint,
	}

	// connect to Redis
	if err := redisClient.Connect(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	ttl := int64(instanceTTL)
	if ttl == 0 {
		ttl = 45 // default 45 seconds
	}

	return &RedisServiceDiscovery{
		client:      redisClient,
		serviceName: serviceName,
		instanceTTL: ttl,
		instances:   []string{},
	}, nil
}

// GetNextInstance returns the next available instance (round-robin)
func (d *RedisServiceDiscovery) GetNextInstance() (string, error) {
	d.instancesLock.RLock()
	instanceCount := len(d.instances)
	d.instancesLock.RUnlock()

	if instanceCount == 0 {
		// refresh the instance list
		if err := d.Refresh(); err != nil {
			return "", fmt.Errorf("failed to refresh instances: %w", err)
		}

		d.instancesLock.RLock()
		instanceCount = len(d.instances)
		d.instancesLock.RUnlock()

		if instanceCount == 0 {
			return "", fmt.Errorf("no available instances for service '%s'", d.serviceName)
		}
	}

	// atomically increment and get the instance
	index := atomic.AddUint32(&d.currentIndex, 1)

	d.instancesLock.RLock()
	instance := d.instances[int(index)%len(d.instances)]
	d.instancesLock.RUnlock()

	return instance, nil
}

// Refresh refreshes the instance list
func (d *RedisServiceDiscovery) Refresh() error {
	key := fmt.Sprintf("grpc:services:%s", d.serviceName)

	// get all instances from Redis
	result, _, err := d.client.HASH.HGetAll(key)
	if err != nil {
		return fmt.Errorf("failed to get instances from redis: %w", err)
	}

	now := time.Now().Unix()
	var validInstances []string

	// parse and filter valid instances
	for instanceID, data := range result {
		var info RedisInstanceInfo

		if err := json.Unmarshal([]byte(data), &info); err != nil {
			log.Printf("[Redis Discovery] Failed to unmarshal instance %s: %v", instanceID, err)
			continue
		}

		// check whether expired
		if now-info.LastUpdate > d.instanceTTL {
			log.Printf("[Redis Discovery] Instance %s expired (lastUpdate: %d, now: %d, ttl: %d)",
				instanceID, info.LastUpdate, now, d.instanceTTL)
			// client proactively deletes expired instances
			go d.removeInstance(instanceID)
			continue
		}

		validInstances = append(validInstances, fmt.Sprintf("%s:%d", info.IP, info.Port))
	}

	d.instancesLock.Lock()
	d.instances = validInstances
	d.instancesLock.Unlock()

	log.Printf("[Redis Discovery] Refreshed instances for service '%s': %d instances found", d.serviceName, len(validInstances))

	return nil
}

// RemoveFailedInstance removes an instance that failed to connect
func (d *RedisServiceDiscovery) RemoveFailedInstance(addr string) error {
	// remove from local cache
	d.instancesLock.Lock()
	for i, instance := range d.instances {
		if instance == addr {
			d.instances = append(d.instances[:i], d.instances[i+1:]...)
			log.Printf("[Redis Discovery] Removed failed instance from cache: %s", addr)
			break
		}
	}
	d.instancesLock.Unlock()

	// find and delete the matching instance from Redis
	key := fmt.Sprintf("grpc:services:%s", d.serviceName)
	result, _, err := d.client.HASH.HGetAll(key)
	if err != nil {
		return fmt.Errorf("failed to get instances from redis: %w", err)
	}

	// find the matching instanceID
	for instanceID, data := range result {
		var info RedisInstanceInfo
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			continue
		}

		instanceAddr := fmt.Sprintf("%s:%d", info.IP, info.Port)
		if instanceAddr == addr {
			if _, err := d.client.HASH.HDel(key, instanceID); err != nil {
				return fmt.Errorf("failed to remove instance from redis: %w", err)
			}
			log.Printf("[Redis Discovery] Removed failed instance from Redis: %s (ID: %s)", addr, instanceID)
			return nil
		}
	}

	return nil
}

// removeInstance deletes expired instances from Redis (runs in background)
func (d *RedisServiceDiscovery) removeInstance(instanceID string) {
	key := fmt.Sprintf("grpc:services:%s", d.serviceName)

	if _, err := d.client.HASH.HDel(key, instanceID); err != nil {
		log.Printf("[Redis Discovery] Failed to remove expired instance %s: %v", instanceID, err)
	} else {
		log.Printf("[Redis Discovery] Removed expired instance %s from Redis", instanceID)
	}
}

// GetAllInstances returns all cached instance addresses (Refresh first, then return a copy)
func (d *RedisServiceDiscovery) GetAllInstances() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}

	d.instancesLock.RLock()
	defer d.instancesLock.RUnlock()

	if len(d.instances) == 0 {
		return nil, fmt.Errorf("no available instances for service '%s'", d.serviceName)
	}

	out := make([]string, len(d.instances))
	copy(out, d.instances)
	return out, nil
}

// GetInstanceCount returns the current number of available instances
func (d *RedisServiceDiscovery) GetInstanceCount() int {
	d.instancesLock.RLock()
	defer d.instancesLock.RUnlock()
	return len(d.instances)
}

// Close closes the Redis connection
func (d *RedisServiceDiscovery) Close() error {
	if d.client != nil {
		return nil
	}
	return nil
}
