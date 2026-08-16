--[[
breaker_probe.lua
=================
Determine whether a probe request may be sent when the breaker is open,
implementing the closed → open → half_open → closed (or open) cycle.

KEYS[1]  = breaker:{device_id}:state   — String key; exists when breaker is open
KEYS[2]  = breaker:{device_id}:probe   — String key; exists when a probe is in flight

ARGV[1]  = half_open_ttl_seconds (int) — TTL for the probe key; one probe at a time

Returns (string):
    "closed"     — state key does not exist; breaker is closed; proceed normally
    "half_open"  — state exists but no probe is in flight; caller may send probe
    "open"       — state exists and a probe is already in flight; caller must wait

Behaviour:
- If the state key does NOT exist → return "closed"
  (The state key either never existed or its TTL has expired, meaning the
  self-healing reset window has passed and the breaker auto-recovers.)
- If the state key EXISTS and the probe key does NOT exist:
      Set the probe key (NX, EX = half_open_ttl) → return "half_open"
      This races safely: SET NX is atomic, so exactly one caller wins.
- If the state key EXISTS and the probe key EXISTS:
      A probe is already in flight → return "open" (caller backs off)

Notes:
- The probe key TTL defines how long to wait for a probe result before
  allowing another probe attempt.
- On a successful probe, the Python layer calls DeviceCircuitBreaker.reset()
  which deletes both the state key and the probe key.
- On a failed probe, the Python layer leaves the state key in place (it will
  expire on its own) and deletes the probe key so another probe can fire
  after the half_open_ttl.
--]]

local state_key = KEYS[1]
local probe_key = KEYS[2]
local probe_ttl = tonumber(ARGV[1])

local state_exists = redis.call("EXISTS", state_key)

if state_exists == 0 then
    -- Breaker is closed (state key expired or never set).
    return "closed"
end

-- Breaker is open.  Try to claim a probe slot atomically.
local probe_set = redis.call("SET", probe_key, "1", "NX", "EX", probe_ttl)

if probe_set ~= false then
    -- We won the race: this caller may send the probe.
    return "half_open"
else
    -- Another probe is already in flight.
    return "open"
end
