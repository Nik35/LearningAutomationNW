--[[
semaphore_renew.lua
===================
Heartbeat renewal: if the worker's slot still exists, refresh the TTL on the
entire semaphore hash so it does not expire while the worker is active.

KEYS[1]  = sem:{device_id}       — the Redis Hash that tracks active slots
ARGV[1]  = worker_id             — field to check
ARGV[2]  = new_ttl_seconds (int) — fresh TTL to apply to the key

Returns:
    1  — worker's field exists; TTL refreshed
    0  — worker's field is gone (slot was already reclaimed by TTL expiry or
         an explicit release); the caller should treat this as a lost slot
         and abort the workflow to avoid running without a concurrency guard.

Notes:
- We check with HEXISTS before refreshing.  If the field is absent it means
  the key expired (and was recreated empty, or was deleted) while the worker
  was still alive — this is an abnormal condition and must not be silently
  ignored by the heartbeat renewer.
- Only EXPIRE is set; the stored acquired_at timestamp is not updated so the
  original acquisition time remains auditable.
--]]

local key       = KEYS[1]
local worker_id = ARGV[1]
local new_ttl   = tonumber(ARGV[2])

local exists = redis.call("HEXISTS", key, worker_id)
if exists == 1 then
    redis.call("EXPIRE", key, new_ttl)
    return 1
else
    return 0
end
