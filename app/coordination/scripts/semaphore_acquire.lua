--[[
semaphore_acquire.lua
=====================
Atomically try to acquire one slot in the per-device semaphore.

KEYS[1]  = sem:{device_id}          — the Redis Hash that tracks active slots
ARGV[1]  = max_slots (integer)      — P-1: per-device concurrency limit
ARGV[2]  = slot_ttl_seconds (int)   — TTL on the whole hash key; renewed by heartbeat
ARGV[3]  = worker_id (string)       — unique identifier of this worker

Hash layout:
    field = worker_id
    value = acquired_at (Unix timestamp as integer)

Returns:
    1  — slot acquired
    0  — all slots occupied; caller should back off and retry

Notes:
- EXPIRE is set on the *whole hash* key, not per field. This means if no
  heartbeat renews the key, the entire semaphore key expires and all slots
  are automatically reclaimed. Each worker's heartbeat calls semaphore_renew.lua.
- We count only fields currently in the hash. An expired key returns an empty
  hash, so HLEN returns 0 and the next acquire succeeds — no explicit cleanup
  of stale fields is needed when TTL is properly maintained.
--]]

local key       = KEYS[1]
local max_slots = tonumber(ARGV[1])
local ttl       = tonumber(ARGV[2])
local worker_id = ARGV[3]

-- Count how many slots are currently held.
local current = redis.call("HLEN", key)

if current < max_slots then
    -- Slot available: record acquisition timestamp and set/refresh TTL.
    local now = redis.call("TIME")
    -- TIME returns {seconds, microseconds}; use seconds for the stored timestamp.
    local acquired_at = now[1]
    redis.call("HSET", key, worker_id, acquired_at)
    redis.call("EXPIRE", key, ttl)
    return 1
else
    -- All slots occupied.
    return 0
end
