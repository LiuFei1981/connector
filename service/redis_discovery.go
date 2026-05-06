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

// RedisServiceRegistry 服务注册器，负责将 gRPC 服务实例注册到 Redis
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

// RedisInstanceInfo Redis 中存储的实例信息
type RedisInstanceInfo struct {
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	LastUpdate int64  `json:"lastUpdate"`
}

// NewRedisServiceRegistry 创建 Redis 服务注册器
func NewRedisServiceRegistry(cfg *redisData, serviceName string, ip string, port uint) (*RedisServiceRegistry, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if len(cfg.WriteEndpoint) == 0 {
		return nil, fmt.Errorf("redis write endpoints not configured")
	}

	if len(cfg.ReadEndpoint) == 0 {
		return nil, fmt.Errorf("redis read endpoints not configured")
	}

	// 创建 Redis 客户端
	writerEndpoint := cfg.WriteEndpoint
	readerEndpoint := cfg.ReadEndpoint

	redisClient := &redis.Redis{
		AwsRedisWriterEndpoint: writerEndpoint,
		AwsRedisReaderEndpoint: readerEndpoint,
	}

	// 连接 Redis
	if err := redisClient.Connect(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	// 生成实例 ULID
	instanceID := util.NewULID()

	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 30 // 默认 30 秒
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

// Start 启动心跳注册
func (r *RedisServiceRegistry) Start() error {
	if r == nil {
		return nil
	}

	if r.isRunning {
		return fmt.Errorf("registry already running")
	}

	// 立即注册一次
	if err := r.register(); err != nil {
		return fmt.Errorf("initial registration failed: %w", err)
	}

	// 启动心跳 goroutine
	r.isRunning = true
	go r.heartbeatLoop()

	log.Printf("[Redis Discovery] Service '%s' registered with instance ID '%s' at %s:%d",
		r.serviceName, r.instanceID, r.ip, r.port)

	return nil
}

// Stop 停止心跳并注销实例
func (r *RedisServiceRegistry) Stop() error {
	if r == nil {
		return nil
	}

	if !r.isRunning {
		return nil
	}

	// 停止心跳
	close(r.stopChan)
	r.heartbeatTicker.Stop()
	r.isRunning = false

	// 从 Redis 删除实例信息
	if err := r.deregister(); err != nil {
		log.Printf("[Redis Discovery] Failed to deregister instance %s: %v", r.instanceID, err)
		return err
	}

	log.Printf("[Redis Discovery] Service '%s' instance '%s' deregistered", r.serviceName, r.instanceID)

	return nil
}

// heartbeatLoop 心跳循环
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

// register 注册或更新实例信息到 Redis
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

	// 使用 HASH.HSet 存储实例信息
	if err := r.client.HASH.HSet(key, r.instanceID, string(data)); err != nil {
		return fmt.Errorf("failed to set instance info: %w", err)
	}

	//check redis info
	result, _, err := r.client.HASH.HGetAll(key)
	if err != nil {
		return fmt.Errorf("failed to get instances from redis: %w", err)
	}

	for instanceID, readData := range result {
		
		println(fmt.Sprintf("reids data key:  instanceid:  data:", key, instanceID, readData))
	}
	return nil
}

// deregister 从 Redis 注销实例
func (r *RedisServiceRegistry) deregister() error {
	key := r.getRedisKey()

	// 使用 HASH.HDel 删除实例信息
	if _, err := r.client.HASH.HDel(key, r.instanceID); err != nil {
		return fmt.Errorf("failed to delete instance info: %w", err)
	}

	return nil
}

// getRedisKey 获取 Redis key
func (r *RedisServiceRegistry) getRedisKey() string {
	return fmt.Sprintf("grpc:services:%s", r.serviceName)
}

// GetInstanceID 获取实例 ID
func (r *RedisServiceRegistry) GetInstanceID() string {
	if r == nil {
		return ""
	}
	return r.instanceID
}

// GetServiceName 获取服务名称
func (r *RedisServiceRegistry) GetServiceName() string {
	if r == nil {
		return ""
	}
	return r.serviceName
}
