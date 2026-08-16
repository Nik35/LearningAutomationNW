--[[
semaphore_release.lua
=====================
Atomically release one semaphore slot by removing the worker's field from the hash.

KEYS[1]  = sem:{device_id}   — the Redis Hash that tracks active slots
ARGV[1]  = worker_id         — field to remove

Returns:
    1  — field was present and has been deleted
    0  — field was not found (already released or TTL expired); idempotent

This operation is intentionally idempotent: calling it twice for the same
worker_id is safe and does not error.  The "finally" block in DeviceSemaphore.slot()
always calls release, so this must tolerate being called after an expiry.
--]]

local key       = KEYS[1]
local worker_id = ARGV[1]

local deleted = redis.call("HDEL", key, worker_id)
return deleted
