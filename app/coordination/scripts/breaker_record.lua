--[[
breaker_record.lua
==================
Record one call outcome into the circuit breaker's sliding-window statistics
and trip the breaker if thresholds are exceeded.

KEYS[1]  = breaker:{device_id}:stats   — Hash storing aggregate statistics
KEYS[2]  = breaker:{device_id}:events  — List of recent event entries (sliding window)
KEYS[3]  = breaker:{device_id}:state   — String key for circuit state ("open")

ARGV[1]  = outcome          — "success" | "failure" | "timeout"
ARGV[2]  = latency_ms       — call latency in milliseconds (number as string)
ARGV[3]  = now_unix         — current Unix timestamp as integer (seconds)
ARGV[4]  = window_seconds   — sliding window size in seconds
ARGV[5]  = error_rate_threshold      — trip if errors/total > this (0.0–1.0)
ARGV[6]  = timeout_rate_threshold    — trip if timeouts/total > this (0.0–1.0)
ARGV[7]  = p95_latency_threshold_ms  — trip if approx p95 > this (ms)
ARGV[8]  = open_state_ttl_seconds    — TTL for the "open" state key when breaker trips

Event entry format stored in the list (pipe-delimited):
    "{timestamp}|{outcome}|{latency_ms}"

Returns (as a Redis status / bulk string):
    "closed"    — breaker remains / stays closed
    "open"      — breaker just tripped or was already open

Algorithm:
1. RPUSH the new event.
2. Trim the list to only events within the window by popping from the left
   until the oldest entry is within [now - window_seconds, now].
3. Walk the remaining entries and compute:
       total, errors (failure+timeout), timeouts, latency list
4. Compute error_rate, timeout_rate, approx p95 (sort latencies, index 0.95*n).
5. If any threshold exceeded AND state key does not already exist: set state="open"
   with the configured TTL to represent a self-healing reset period.
6. Return current state.

Notes:
- The list approach gives an exact sliding window at the cost of O(n) trim on each
  call. For the expected window sizes (tens to low hundreds of events) this is fine.
- p95 approximation: sort the latency list and take the element at index
  floor(0.95 * count). Accurate enough for breaker decisions; not a true histogram.
- The state key TTL acts as the half-open window; breaker_probe.lua manages probes.
--]]

local stats_key  = KEYS[1]
local events_key = KEYS[2]
local state_key  = KEYS[3]

local outcome              = ARGV[1]
local latency_ms           = tonumber(ARGV[2])
local now                  = tonumber(ARGV[3])
local window_seconds       = tonumber(ARGV[4])
local err_rate_threshold   = tonumber(ARGV[5])
local tmo_rate_threshold   = tonumber(ARGV[6])
local p95_threshold_ms     = tonumber(ARGV[7])
local open_ttl             = tonumber(ARGV[8])

-- 1. Append the new event to the list.
local entry = tostring(now) .. "|" .. outcome .. "|" .. tostring(latency_ms)
redis.call("RPUSH", events_key, entry)
-- Keep the events key alive as long as the window, plus a small buffer.
redis.call("EXPIRE", events_key, window_seconds + 60)

-- 2. Trim old events from the left of the list.
--    We do this iteratively: peek at LINDEX 0, pop if too old.
local cutoff = now - window_seconds
while true do
    local oldest = redis.call("LINDEX", events_key, 0)
    if oldest == false then
        break
    end
    -- Parse timestamp (first pipe-delimited field).
    local ts_end = string.find(oldest, "|", 1, true)
    local ts = tonumber(string.sub(oldest, 1, ts_end - 1))
    if ts < cutoff then
        redis.call("LPOP", events_key)
    else
        break
    end
end

-- 3. Walk remaining events to compute metrics.
local all_events = redis.call("LRANGE", events_key, 0, -1)
local total    = 0
local errors   = 0
local timeouts = 0
local latencies = {}

for _, ev in ipairs(all_events) do
    local parts = {}
    local pat = "([^|]+)"
    for part in string.gmatch(ev, pat) do
        table.insert(parts, part)
    end
    -- parts = { timestamp, outcome, latency_ms }
    if #parts == 3 then
        total = total + 1
        local ev_outcome = parts[2]
        local ev_lat = tonumber(parts[3])
        if ev_outcome == "failure" or ev_outcome == "timeout" then
            errors = errors + 1
        end
        if ev_outcome == "timeout" then
            timeouts = timeouts + 1
        end
        if ev_lat ~= nil then
            table.insert(latencies, ev_lat)
        end
    end
end

-- 4. Compute rates and approximate p95.
local should_trip = false

if total > 0 then
    local error_rate   = errors   / total
    local timeout_rate = timeouts / total

    if error_rate > err_rate_threshold then
        should_trip = true
    end
    if timeout_rate > tmo_rate_threshold then
        should_trip = true
    end
end

-- Approximate p95 from sorted latency list.
if #latencies > 0 then
    table.sort(latencies)
    local p95_idx = math.floor(0.95 * #latencies)
    if p95_idx < 1 then p95_idx = 1 end
    local p95_lat = latencies[p95_idx]
    if p95_lat > p95_threshold_ms then
        should_trip = true
    end
end

-- 5. Trip the breaker if thresholds exceeded and it is not already open.
if should_trip then
    -- SET NX so we do not overwrite an already-open breaker (and extend its TTL).
    -- If the breaker is already open the probe key governs the half-open transition.
    local already_open = redis.call("EXISTS", state_key)
    if already_open == 0 then
        redis.call("SET", state_key, "open", "EX", open_ttl)
    end
    return "open"
end

-- 6. Return closed (state_key may or may not exist — probe.lua handles half-open).
local state_exists = redis.call("EXISTS", state_key)
if state_exists == 1 then
    return "open"
end
return "closed"
