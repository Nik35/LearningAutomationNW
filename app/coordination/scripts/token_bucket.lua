--[[
token_bucket.lua
================
Atomic token-bucket rate limiter (per device).

KEYS[1]  = bucket:{device_id}              — the Redis Hash storing bucket state
ARGV[1]  = capacity (number)               — P-2: max tokens; burst ceiling
ARGV[2]  = refill_rate_per_sec (number)    — P-3: tokens added per second
ARGV[3]  = tokens_requested (number)       — how many tokens this call costs (usually 1)
ARGV[4]  = now_unix_float (string/number)  — current time as Unix float seconds

Hash fields:
    tokens      — current token count (float stored as string)
    last_refill — Unix timestamp of the last refill (float as string)

Returns:
    1  — request allowed; tokens have been deducted
    0  — request rejected; bucket is empty (tokens saved after refill, not deducted)

Algorithm (standard token bucket):
    1. Read last_tokens and last_refill_time.
       If key does not exist, initialise to full capacity.
    2. elapsed = now - last_refill_time
    3. new_tokens = min(capacity, last_tokens + elapsed * refill_rate)
    4. If new_tokens >= tokens_requested:
           deduct and store → return 1
       Else:
           store refilled-but-not-granted count → return 0

TTL: set to ceil(capacity / refill_rate) * 2 so that an idle bucket key is
cleaned up automatically.  A minimum of 60 seconds prevents rapid churn on
high-rate buckets.
--]]

local key              = KEYS[1]
local capacity         = tonumber(ARGV[1])
local refill_rate      = tonumber(ARGV[2])
local tokens_requested = tonumber(ARGV[3])
local now              = tonumber(ARGV[4])

-- Read current state from the hash.
local raw_tokens      = redis.call("HGET", key, "tokens")
local raw_last_refill = redis.call("HGET", key, "last_refill")

local last_tokens
local last_refill

if raw_tokens == false or raw_last_refill == false then
    -- Key does not exist or is partially initialised: start with a full bucket.
    last_tokens = capacity
    last_refill = now
else
    last_tokens = tonumber(raw_tokens)
    last_refill = tonumber(raw_last_refill)
end

-- Compute tokens added since last refill.
local elapsed   = now - last_refill
local new_tokens = last_tokens + elapsed * refill_rate
if new_tokens > capacity then
    new_tokens = capacity
end

-- Determine TTL: 2 × the time it takes to refill from zero.
-- Guard against division by zero (refill_rate should always be > 0).
local refill_period = capacity / refill_rate
local key_ttl = math.ceil(refill_period * 2)
if key_ttl < 60 then
    key_ttl = 60
end

local allowed
if new_tokens >= tokens_requested then
    new_tokens = new_tokens - tokens_requested
    allowed = 1
else
    -- Not enough tokens; still persist the refilled count so the next
    -- call does not refill again from scratch.
    allowed = 0
end

-- Persist updated state.
redis.call("HSET", key, "tokens", tostring(new_tokens), "last_refill", tostring(now))
redis.call("EXPIRE", key, key_ttl)

return allowed
