package client

/*
 * Copyright 2020-2026 Aldelo, LP
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
	"context"
	"fmt"
	"log"
	"time"
)

// DialViaRedis is a fallback dial method that bypasses CloudMap service discovery
// and uses Redis to resolve service endpoints. It reads config from the same yaml,
// validates Redis is enabled, discovers all instances from Redis, then tries each
// endpoint in round-robin order until one succeeds. Only returns error when all
// instances have been tried and failed.
//
// Retained from standard Dial: config read/validate, buildDialOptions, WaitForServerReady.
// Skipped: connectSd, discoverEndpoints, load balancer resolver, notifier client, web server.
func (c *Client) DialViaRedis(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if c == nil {
		return fmt.Errorf("Client Object Nil")
	}

	c._lifecycleMu.Lock()
	defer c._lifecycleMu.Unlock()

	// reset lifecycle state
	c.closed.Store(false)
	c.closing.Store(false)
	c.resetClosedCh()
	c.setConnection(nil, "")

	// read config if not yet loaded
	if c.getConfig() == nil {
		if err := c.readConfig(); err != nil {
			return err
		}
	}

	cfg := c.getConfig()
	if cfg == nil {
		return fmt.Errorf("config not loaded")
	}

	// validate Redis config
	if !cfg.Redis.Enabled {
		return fmt.Errorf("redis discovery is not enabled in config")
	}
	if len(cfg.Redis.WriteEndpoint) == 0 {
		return fmt.Errorf("redis write_endpoint not configured")
	}

	z := c.ZLog()
	logf := func(msg string, args ...interface{}) {
		if z != nil {
			z.Printf(msg, args...)
		} else {
			log.Printf(msg, args...)
		}
	}
	errf := func(msg string, args ...interface{}) {
		if z != nil {
			z.Errorf(msg, args...)
		} else {
			log.Printf(msg, args...)
		}
	}

	logf("[DialViaRedis] Starting Redis-based service discovery for %s", cfg.Target.ServiceName)

	// create Redis service discovery client
	rd, err := NewRedisServiceDiscovery(
		cfg.Redis.WriteEndpoint,
		cfg.Redis.ReadEndpoint,
		cfg.Redis.Password,
		cfg.Redis.DB,
		cfg.Target.ServiceName,
		cfg.Redis.InstanceTTL,
	)
	if err != nil {
		return fmt.Errorf("create redis service discovery failed: %w", err)
	}
	c._redisDiscovery = rd

	// get all available instances from Redis
	instances, err := rd.GetAllInstances()
	if err != nil {
		return fmt.Errorf("redis service discovery failed: %w", err)
	}

	logf("[DialViaRedis] Found %d instance(s) from Redis, trying each...", len(instances))

	// build dial options (no load balancer policy for passthrough)
	opts, err := c.buildDialOptions("")
	if err != nil {
		return fmt.Errorf("build dial options failed: %w", err)
	}

	dialSec := cfg.Grpc.DialMinConnectTimeout
	if dialSec == 0 {
		dialSec = defaultDialTimeoutSeconds
	}

	// try each instance in order; on failure move to next
	var lastErr error
	for i, addr := range instances {
		if c.closed.Load() {
			return fmt.Errorf("client closed during DialViaRedis")
		}

		target := fmt.Sprintf("%s:///%s", "passthrough", addr)
		logf("[DialViaRedis] Attempting %d/%d: %s", i+1, len(instances), target)

		dialCtx, cancel := context.WithTimeout(ctx, time.Duration(dialSec)*time.Second)
		conn, dialErr := muxDialContext(dialCtx, target, opts...)
		cancel()

		if dialErr != nil {
			errf("[DialViaRedis] Dial failed for %s: %v", addr, dialErr)
			lastErr = dialErr
			rd.RemoveFailedInstance(addr)
			continue
		}

		// dial succeeded, set connection
		c.setConnection(conn, target)

		// health check if configured
		if c.WaitForServerReady {
			healthCtx, healthCancel := context.WithTimeout(ctx, time.Duration(dialSec)*time.Second)
			healthErr := c.waitForEndpointReady(healthCtx, time.Duration(dialSec)*time.Second)
			healthCancel()

			if healthErr != nil {
				errf("[DialViaRedis] Health check failed for %s: %v", addr, healthErr)
				if closedConn := c.clearConnection(); closedConn != nil {
					_ = closedConn.Close()
				}
				lastErr = healthErr
				rd.RemoveFailedInstance(addr)
				continue
			}
		}

		// success
		logf("[DialViaRedis] Connected to %s", addr)

		if c.BeforeClientDial != nil {
			safeCall("BeforeClientDial", func() { c.BeforeClientDial(c) })
		}
		if c.AfterClientDial != nil {
			safeCall("AfterClientDial", func() { c.AfterClientDial(c) })
		}

		return nil
	}

	// all instances failed
	return fmt.Errorf("all %d redis instances failed for service '%s': last error: %w",
		len(instances), cfg.Target.ServiceName, lastErr)
}

// CloseRedisDiscovery closes the Redis discovery client if active.
// Called automatically by Close() but can also be called manually.
func (c *Client) CloseRedisDiscovery() {
	if c == nil {
		return
	}
	if c._redisDiscovery != nil {
		_ = c._redisDiscovery.Close()
		c._redisDiscovery = nil
	}
}

// RedisDiscovery returns the current Redis service discovery instance (may be nil).
func (c *Client) RedisDiscovery() *RedisServiceDiscovery {
	if c == nil {
		return nil
	}
	return c._redisDiscovery
}
