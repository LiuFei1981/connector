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

// RedisServiceDiscovery 客户端 Redis 服务发现
type RedisServiceDiscovery struct {
	client        *redis.Redis
	serviceName   string
	instanceTTL   int64 // 实例过期时间(秒)
	instances     []string
	instancesLock sync.RWMutex
	currentIndex  uint32
}

// RedisInstanceInfo Redis 中存储的实例信息
type RedisInstanceInfo struct {
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	LastUpdate int64  `json:"lastUpdate"`
}

// NewRedisServiceDiscovery 创建 Redis 服务发现客户端
func NewRedisServiceDiscovery(endpoints []string, password string, db int, serviceName string, instanceTTL uint) (*RedisServiceDiscovery, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("redis endpoints not configured")
	}

	writerEndpoint := endpoints[0]
	readerEndpoint := endpoints[0]

	// 如果有多个端点，第二个作为 reader
	if len(endpoints) > 1 {
		readerEndpoint = endpoints[1]
	}

	redisClient := &redis.Redis{
		AwsRedisWriterEndpoint: writerEndpoint,
		AwsRedisReaderEndpoint: readerEndpoint,
	}

	// 连接 Redis
	if err := redisClient.Connect(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	ttl := int64(instanceTTL)
	if ttl == 0 {
		ttl = 45 // 默认 45 秒
	}

	return &RedisServiceDiscovery{
		client:      redisClient,
		serviceName: serviceName,
		instanceTTL: ttl,
		instances:   []string{},
	}, nil
}

// GetNextInstance 获取下一个可用实例（轮询）
func (d *RedisServiceDiscovery) GetNextInstance() (string, error) {
	d.instancesLock.RLock()
	instanceCount := len(d.instances)
	d.instancesLock.RUnlock()

	if instanceCount == 0 {
		// 刷新实例列表
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

	// 原子递增并获取实例
	index := atomic.AddUint32(&d.currentIndex, 1)

	d.instancesLock.RLock()
	instance := d.instances[int(index)%len(d.instances)]
	d.instancesLock.RUnlock()

	return instance, nil
}

// Refresh 刷新实例列表
func (d *RedisServiceDiscovery) Refresh() error {
	key := fmt.Sprintf("grpc:services:%s", d.serviceName)

	// 从 Redis 获取所有实例
	result, _, err := d.client.HASH.HGetAll(key)
	if err != nil {
		return fmt.Errorf("failed to get instances from redis: %w", err)
	}

	now := time.Now().Unix()
	var validInstances []string

	// 解析并过滤有效实例
	for instanceID, data := range result {
		var info RedisInstanceInfo

		if err := json.Unmarshal([]byte(data), &info); err != nil {
			log.Printf("[Redis Discovery] Failed to unmarshal instance %s: %v", instanceID, err)
			continue
		}

		// 检查是否过期
		if now-info.LastUpdate > d.instanceTTL {
			log.Printf("[Redis Discovery] Instance %s expired (lastUpdate: %d, now: %d, ttl: %d)",
				instanceID, info.LastUpdate, now, d.instanceTTL)
			// 客户端主动删除过期实例
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

// RemoveFailedInstance 移除连接失败的实例
func (d *RedisServiceDiscovery) RemoveFailedInstance(addr string) error {
	// 从本地缓存移除
	d.instancesLock.Lock()
	for i, instance := range d.instances {
		if instance == addr {
			d.instances = append(d.instances[:i], d.instances[i+1:]...)
			log.Printf("[Redis Discovery] Removed failed instance from cache: %s", addr)
			break
		}
	}
	d.instancesLock.Unlock()

	// 从 Redis 查找并删除对应的实例
	key := fmt.Sprintf("grpc:services:%s", d.serviceName)
	result, _, err := d.client.HASH.HGetAll(key)
	if err != nil {
		return fmt.Errorf("failed to get instances from redis: %w", err)
	}

	// 查找匹配的 instanceID
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

// removeInstance 从 Redis 删除过期实例（后台执行）
func (d *RedisServiceDiscovery) removeInstance(instanceID string) {
	key := fmt.Sprintf("grpc:services:%s", d.serviceName)

	if _, err := d.client.HASH.HDel(key, instanceID); err != nil {
		log.Printf("[Redis Discovery] Failed to remove expired instance %s: %v", instanceID, err)
	} else {
		log.Printf("[Redis Discovery] Removed expired instance %s from Redis", instanceID)
	}
}

// GetInstanceCount 获取当前可用实例数量
func (d *RedisServiceDiscovery) GetInstanceCount() int {
	d.instancesLock.RLock()
	defer d.instancesLock.RUnlock()
	return len(d.instances)
}

// Close 关闭 Redis 连接
func (d *RedisServiceDiscovery) Close() error {
	if d.client != nil {
		return nil
	}
	return nil
}
