# Redis — Complete Guide for Architects
## From Basics to Production-Grade Distributed Systems

**Study starts:** tomorrow (2026-08-18)
**Study plan:** at the end of this document

---

## The one mental model you need before everything else

Redis is a **single-threaded, in-memory state machine with a network interface**.

Every command — no matter which client, which pod, which datacenter — runs one at a time
on a single thread. There is no lock contention, no deadlocks, no parallel execution.
This is why it is fast (no context switching) and why Lua scripts give you true atomicity
(nothing else runs while the script is executing).

Everything else in this guide follows from that model.

---

## Part 1 — Data Structures

Redis is not a key-value store. It is a data structure server. The structure you choose
determines which algorithms you can implement and what the performance guarantees are.

### String

The simplest structure. Stores bytes — can be text, JSON, a serialised object, or a number.

```
SET user:1001:name "Nikhil"         → store
GET user:1001:name                  → "Nikhil"
INCR api:rate:user:1001             → atomic increment (no race condition)
INCRBY api:rate:user:1001 5         → increment by 5
SETEX session:abc123 3600 "data"    → set with TTL (expires in 1 hour)
SET lock:job:42 "pod-a" NX EX 30    → set only if not exists (distributed lock)
```

**When to use:** Caching, counters, rate limit counts, session tokens, distributed locks,
simple flags, idempotency keys.

**Architect note:** `INCR` is atomic. Two pods doing `INCR` simultaneously is always safe —
you will never lose a count. This is the simplest form of Redis coordination.

---

### Hash

A map of field→value pairs under one key. Like a Python dict or a database row.

```
HSET user:1001 name "Nikhil" role "architect" active "true"
HGET user:1001 name             → "Nikhil"
HGETALL user:1001               → all fields and values
HINCRBY user:1001 login_count 1 → atomic increment on one field
HLEN user:1001                  → number of fields (O(1))
HDEL user:1001 active           → remove one field
```

**When to use:** Objects with multiple fields (user profile, session data), grouping related
counters (per-device metrics), per-worker slot tracking (semaphore — field = worker_id,
value = timestamp).

**Why HLEN is important:** O(1) regardless of how many fields exist. If you store a
semaphore as a Hash (`HLEN` tells you how many slots are taken), the slot count check
costs the same whether 1 or 1000 workers hold slots.

**Architect note:** `HGETALL` on a large Hash blocks the server. Never call it in a hot path.
Use `HSCAN` for large hashes or only fetch specific fields with `HMGET`.

---

### List

An ordered sequence. Insert at head (`LPUSH`) or tail (`RPUSH`). Remove from either end.

```
RPUSH jobs:queue "job:1" "job:2"    → add to tail
LPOP jobs:queue                     → remove from head (FIFO queue)
BRPOP jobs:queue 30                 → blocking pop — wait up to 30s for an item
LRANGE jobs:queue 0 -1              → get all items (use SCAN for large lists)
LLEN jobs:queue                     → length
```

**When to use:** Task queues, message passing between processes, activity feeds (bounded
list of recent events), undo history.

**Real example — Twitter timeline:** Each user has a List of tweet IDs. When you tweet,
your tweet ID is pushed to the front of each follower's List (LPUSH). When followers
open their feed, they read the first 800 entries (LRANGE 0 799). The List is capped at 800
with `LTRIM` — anything older is dropped (they load from the database instead).

**Architect note:** Lists are sequential. Random access is O(N). If you need to find
"does this item exist?", Lists are wrong. Use a Set.

---

### Set

An unordered collection of unique strings. O(1) add, remove, and membership check.

```
SADD user:1001:friends "1002" "1003" "1004"
SISMEMBER user:1001:friends "1003"     → 1 (yes)
SMEMBERS user:1001:friends             → all members (avoid on large sets)
SCARD user:1001:friends                → count
SINTER user:1001:friends user:1002:friends  → common friends
SUNION user:1001:friends user:1002:friends  → all friends (union)
SDIFF  user:1001:friends user:1002:friends  → friends of 1001 not shared with 1002
```

**When to use:** Tracking unique visitors, tagging systems, friend/follower lists (for
intersection queries), deduplication, permission sets.

**Real example — Stripe:** Payment idempotency keys are stored in a Set per customer.
Before processing a payment, `SISMEMBER` checks if this key was already processed.

**Architect note:** `SMEMBERS` on a large Set blocks the server. Use `SSCAN` for large sets.
Set intersection (`SINTER`) runs in O(N×M) — avoid on large sets in hot paths.

---

### Sorted Set (ZSet)

Like a Set but every member has a floating-point score. Members are always ordered by score.
This is Redis's most powerful data structure.

```
ZADD leaderboard 9850 "player:alice" 9200 "player:bob" 8100 "player:carol"
ZRANK leaderboard "player:bob"           → 1  (0-indexed, ascending)
ZREVRANK leaderboard "player:bob"        → 1  (descending — rank from top)
ZSCORE leaderboard "player:bob"          → 9200.0
ZRANGE leaderboard 0 2 WITHSCORES        → top 3 with scores
ZRANGEBYSCORE leaderboard 9000 +inf      → players with score > 9000
ZINCRBY leaderboard 150 "player:bob"     → atomic score increment
ZCARD leaderboard                        → count
```

**When to use:** Leaderboards, rate limiting (score = timestamp, members = request IDs),
priority queues, geospatial indexing (score = encoded lat/lon), time-series data
(score = unix timestamp).

**Real example — Uber driver locations:** Each geographic cell (H3 hexagon) has a Sorted
Set where members = driver IDs and scores = last-update timestamp. To find nearby drivers:
query the Sorted Set for that cell. To detect ghost drivers (not updating): `ZRANGEBYSCORE`
for scores older than 30 seconds and remove them. Handles millions of updates per second.

**Real example — Rate limiting:** Score = request timestamp. Members = unique request IDs.
```
ZADD requests:{user} {now} {request_id}  → log the request
ZREMRANGEBYSCORE requests:{user} 0 {now-60}  → remove older than 60s
ZCARD requests:{user}                    → count in last 60s
```
If count > limit, reject. Atomic when done in Lua.

**Architect note:** The sorted set is backed by a skip list — insert, delete, and rank are
O(log N). Range queries are O(log N + M) where M is the result size. Extremely fast even
at millions of members.

---

### Stream

A log-like data structure. Append-only. Supports consumer groups (multiple workers, each
gets different messages, acknowledged on completion). Introduced in Redis 5.0.

```
XADD events:orders * action "created" order_id "9001"   → append event
XREAD COUNT 10 BLOCK 0 STREAMS events:orders 0           → read 10 events, blocking
XGROUP CREATE events:orders workers $                     → create consumer group
XREADGROUP GROUP workers pod-a COUNT 5 STREAMS events:orders >  → claim 5 unread
XACK events:orders workers {message_id}                  → acknowledge processed
XPENDING events:orders workers - + 10                    → check unacknowledged
```

**When to use:** Durable event streaming (like Kafka, but simpler), audit logs, async
task processing with guaranteed delivery and acknowledgement.

**Architect note:** Streams are to Pub/Sub what Kafka is to RabbitMQ. Pub/Sub is
fire-and-forget; Streams are durable and support consumer groups with at-least-once
delivery. Choose Streams over Lists for task queues in new projects.

---

### HyperLogLog

Estimates cardinality (count of unique items) using ~12KB of memory regardless of dataset
size. Has ~0.81% error margin. Does not store the actual items — only the count estimate.

```
PFADD unique:visitors "user:1001" "user:1002" "user:1001"  → adds (deduplicates)
PFCOUNT unique:visitors                                      → ~2
PFMERGE all:visitors dc-a:visitors dc-b:visitors            → merge across DCs
```

**When to use:** Counting unique visitors, unique search queries, distinct events —
anywhere you need COUNT DISTINCT at massive scale without storing every item.

**Real example:** A news site tracking unique article readers per day. Each pageview:
`PFADD article:1234:readers:{date} {user_id}`. The count is always accurate to ~1%.
Storing actual user IDs would require gigabytes; HyperLogLog uses 12KB per article per day.

---

### Bitmap

A string interpreted as a sequence of bits. O(1) per-bit operations.

```
SETBIT user:logins:{date} {user_id} 1   → mark user as logged in today
GETBIT user:logins:{date} {user_id}     → did this user log in today?
BITCOUNT user:logins:{date}             → how many users logged in today?
BITOP AND active_users logins:mon logins:tue logins:wed  → users active all 3 days
```

**When to use:** Daily active user tracking, feature flag rollouts by user ID, presence
detection, streak tracking.

**Real example:** A 10-million user system needs to track daily active users. A bitmap with
one bit per user ID is 10M bits = 1.25MB per day. Checking if a specific user is active:
O(1). Counting all active users: O(N/8) but done in C — extremely fast.

---

### Geo (built on Sorted Set)

Stores latitude/longitude pairs. Queries: nearby points within radius, distance between
points. Internally encodes lat/lon as a Sorted Set score using a geohash.

```
GEOADD drivers:dc-a 77.5946 12.9716 "driver:001"     → add driver (lon, lat, member)
GEODIST drivers:dc-a "driver:001" "driver:002" km     → distance in km
GEOSEARCH drivers:dc-a FROMMEMBER "driver:001" BYRADIUS 5 km ASC  → nearby
```

**When to use:** Ride-hailing driver proximity, store locators, delivery radius checks,
location-aware rate limiting.

---

## Part 2 — Core Concepts

### TTL and key expiry

Every key can have an expiration time. Redis expires keys lazily (on access) and
periodically (background sweep). A key past its TTL is deleted; GET returns nil.

```
EXPIRE key 300          → expire in 300 seconds
TTL key                 → seconds remaining (-1 = no TTL, -2 = key doesn't exist)
PERSIST key             → remove TTL (make it permanent)
SET key value EX 300    → set + expire in one atomic command
```

**Architect rule:** Every cache key must have a TTL. Without one, the key lives forever
and Redis grows unboundedly. The only keys that should be permanent are coordination
primitives (semaphore registries, circuit breaker state) that you manage explicitly.

---

### Atomicity — why Lua beats MULTI/EXEC for most things

Redis has two ways to group commands atomically:

**MULTI/EXEC (transactions):**
```
MULTI
INCR counter
EXPIRE counter 60
EXEC
```
Both commands run together. BUT — MULTI/EXEC does not allow conditional logic.
You cannot say "if the counter is over 100, don't increment". You can only queue commands.

**Lua scripts:**
```lua
local count = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], 60)
if tonumber(count) > 100 then
    return 0   -- rejected
end
return 1
```
Lua runs atomically AND allows conditions, loops, and arithmetic. No other Redis command
runs while the script is executing. This is what makes token buckets, semaphores, and
circuit breakers implementable as Redis primitives.

**Rule of thumb:** Use MULTI/EXEC for fire-and-forget grouped writes. Use Lua whenever
you need to read, compute, then write based on the result.

---

### Pub/Sub

Broadcast messages to all subscribers of a channel. Fire-and-forget — if no one is
subscribed, the message is lost. If a subscriber disconnects and reconnects, it misses
messages sent while offline.

```
SUBSCRIBE channel:alerts            → subscribe (blocking)
PUBLISH channel:alerts "text"       → publish to all subscribers
PSUBSCRIBE channel:*                → pattern subscribe
```

**When to use:** Real-time notifications, live dashboards, cache invalidation across pods.
Not for task queues — use Streams instead.

**Real example — Discord:** When a message is sent in a guild, it is published to
`guild:{guild_id}:events`. All gateway pods serving that guild are subscribed and push
the event to connected WebSocket clients.

---

### Persistence — RDB vs AOF

**RDB (Redis Database file):**
- Periodic snapshot of the entire dataset to disk
- Fast to load on restart
- Loses data since the last snapshot on crash (up to minutes)
- Fork-based — transient memory spike during snapshot

**AOF (Append-Only File):**
- Logs every write command
- `appendfsync everysec` — sync to disk once per second (at most 1 second of data loss)
- Slower to load on restart (replays every command)
- Grows over time; AOF rewrite compacts it

**Both together (recommended for production):**
```
appendonly yes
appendfsync everysec
save 900 1      # RDB: save after 900s if at least 1 key changed
save 300 10
save 60 10000
```
On restart, Redis uses the AOF (more complete). RDB serves as a faster backup copy.

**Architect note:** AOF rewrite forks the process. Combined with copy-on-write, memory
usage can nearly double during a rewrite. This is why `maxmemory` must be set to 60–70%
of container memory, not 90%.

---

### Eviction policies

What Redis does when `maxmemory` is reached:

| Policy | Behaviour | Use when |
|---|---|---|
| `noeviction` | Reject writes with OOM error | Task queues, coordination state — nothing must be silently deleted |
| `allkeys-lru` | Evict least recently used key | Pure cache — all keys are expendable |
| `volatile-lru` | Evict LRU among keys with TTL | Mixed: cache (has TTL) + permanent state (no TTL) |
| `allkeys-lfu` | Evict least frequently used | Cache with hot/cold data |
| `volatile-ttl` | Evict keys with soonest TTL | Prefer expiring soon-to-die keys first |
| `allkeys-random` | Evict random key | Almost never the right choice |

**The critical mistake:** Using `allkeys-lru` when Redis also holds Celery task payloads.
Redis evicts a task, worker picks it up and finds nothing, task is silently lost. No error
anywhere. `noeviction` is safer — the write fails, the API catches it, the client retries.

---

## Part 3 — Coordination Patterns (architect-level)

These are what separate Redis users from Redis architects.

### Pattern 1 — Distributed Lock (Redlock)

Guarantees only one process holds a lock across multiple pods.

```python
# Acquire: SET key value NX EX ttl
# NX = only if not exists
# EX = expire after ttl seconds (safety net if holder crashes)

acquired = redis.set(f"lock:{resource}", worker_id, nx=True, ex=30)
if not acquired:
    raise AlreadyLocked()

try:
    do_work()
finally:
    # Release only if we still own it (Lua for atomicity)
    lua = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """
    redis.eval(lua, 1, f"lock:{resource}", worker_id)
```

**Why the Lua release?** Without it, a race exists: your lock expires, another pod acquires
it, then you delete it — you just deleted someone else's lock.

**Architect caution:** Distributed locks with Redis are not perfectly safe under network
partitions (see the Redlock controversy). For most use cases they are fine. For financial
operations requiring strict guarantees, use a consensus system (etcd, ZooKeeper).

---

### Pattern 2 — Rate Limiter (Sliding Window with Sorted Set)

More accurate than fixed windows. Counts actual requests in the last N seconds.

```lua
-- KEYS[1] = rate_limit:{user}, ARGV[1] = limit, ARGV[2] = window_seconds, ARGV[3] = now
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local cutoff = now - window

redis.call('ZREMRANGEBYSCORE', key, 0, cutoff)   -- remove old entries
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, now .. math.random())  -- add this request
    redis.call('EXPIRE', key, window)
    return 1   -- allowed
end
return 0   -- rejected
```

**Real use:** GitHub uses this pattern for API rate limiting: 5000 requests per hour per
authenticated user. Each request is a Sorted Set entry; score = timestamp; old entries
pruned on each check.

---

### Pattern 3 — Counting Semaphore

Cap concurrent access to a shared resource across N pods.

```lua
-- KEYS[1] = sem:{resource}, ARGV[1] = max_slots, ARGV[2] = ttl, ARGV[3] = holder_id
local count = redis.call('HLEN', KEYS[1])
if tonumber(count) < tonumber(ARGV[1]) then
    redis.call('HSET', KEYS[1], ARGV[3], redis.call('TIME')[1])
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1  -- acquired
end
return 0  -- full
```

Self-healing: TTL on the hash means dead holders are automatically cleaned up.
Heartbeat extends the TTL while the holder is alive.

---

### Pattern 4 — Token Bucket (Rate Limit with Burst)

Allows bursts up to capacity, then throttles to refill rate.
Better than fixed-window rate limiting for APIs with bursty-but-reasonable clients.

```lua
local capacity    = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now         = tonumber(ARGV[3])

local data        = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens      = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

local new_tokens  = math.min(capacity, tokens + (now - last_refill) * refill_rate)

if new_tokens >= 1 then
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens - 1, 'last_refill', now)
    return 1
end
redis.call('HMSET', KEYS[1], 'tokens', new_tokens, 'last_refill', now)
return 0
```

**Difference from sliding window:** Token bucket is stateless about individual requests —
it only tracks current token count and last refill time. Sliding window tracks every
individual request in a Sorted Set. Token bucket uses less memory for high-traffic APIs.

---

### Pattern 5 — Circuit Breaker

Cross-pod failure detection for external services.

```
State machine per device:
  CLOSED → [error rate > threshold] → OPEN
  OPEN   → [probe interval elapsed] → HALF_OPEN
  HALF_OPEN → [probe succeeds]      → CLOSED
  HALF_OPEN → [probe fails]         → OPEN
```

Key stored in Redis: `breaker:{device_id}:state` = "CLOSED" | "OPEN" | "HALF_OPEN"
Supporting keys: error counts, latency samples, consecutive timeout counter.

All state updates must be Lua scripts (read-compute-write atomically).

All 4 pods share the same circuit breaker state — one pod detecting a failure opens the
breaker for all pods simultaneously.

---

### Pattern 6 — Pub/Sub for Cache Invalidation

When one pod updates an object in the database, it publishes an invalidation message.
All pods subscribe and evict the local cache entry.

```python
# Writer
db.update(user_id, new_data)
cache.delete(f"user:{user_id}")
redis.publish(f"invalidate:user", str(user_id))

# All pods (at startup)
async def listen():
    async for message in redis.pubsub().subscribe("invalidate:user"):
        local_cache.delete(f"user:{message['data']}")
```

Avoids cache staleness across pods without a short TTL. Stack Overflow uses L1 (in-process)
+ L2 (Redis) caching with this invalidation pattern.

---

### Pattern 7 — Leaderboard with Sorted Set

```python
# Record score
redis.zadd("leaderboard:season:3", {"player:alice": 9850})
redis.zincrby("leaderboard:season:3", 150, "player:alice")  # add points

# Query
rank  = redis.zrevrank("leaderboard:season:3", "player:alice")   # 0-indexed from top
score = redis.zscore("leaderboard:season:3", "player:alice")
top10 = redis.zrevrange("leaderboard:season:3", 0, 9, withscores=True)
```

Real-time, exact ranking across millions of players. Rank query is O(log N).
This is why every major game uses Redis for live leaderboards.

---

## Part 4 — High Availability Architecture

### Sentinel — for most deployments

```
           clients
              │
    ┌─────────┼─────────┐
    │         │         │
Sentinel-1 Sentinel-2 Sentinel-3   (odd number, quorum = 2)
    │
    ├── Primary Redis  (accepts writes)
    └── Replica-1      (read-only, async replication)
        Replica-2
```

- Sentinels monitor the primary; if quorum agrees it is unreachable, they elect a replica
  as the new primary (failover in ~30 seconds)
- Clients connect to Sentinel to discover the current primary address
- **Multi-key atomic operations work** because there is always one primary
- Best choice for single-datacenter or small-to-medium deployments
- **Use this unless your dataset outgrows a single node**

### Redis Cluster — for horizontal scale

```
Shard-1: slots 0–5460       Shard-2: slots 5461–10922     Shard-3: slots 10923–16383
  Primary + Replica           Primary + Replica               Primary + Replica
```

- 16,384 hash slots distributed across N primaries
- Key is hashed to a slot: `slot = CRC16(key) % 16384`
- Each key lives on exactly one shard
- **Multi-key operations only work if all keys are on the same slot**
  (use hash tags `{user:1001}:sessions` to force co-location)
- Writes scale horizontally
- More operationally complex; harder to migrate
- **Use when your dataset is too large for a single node or write throughput saturates one primary**

### Cross-datacenter replication

Redis replication is async — replicas may lag by milliseconds.
For active-active across DCs, use Redis Enterprise's CRDT implementation (commercial).
For read replicas in a second DC, standard replication works. For writes, route all writes
to the primary DC.

**Architecture used in this F5 project:** Single primary (DC-A) + replica (DC-B).
Failover is manual — promote DC-B replica if DC-A primary dies. RPO = replication lag
(seconds). RTO = time to detect + manually promote (minutes).

---

## Part 5 — Memory Management (production critical)

### Key overhead

Every key costs ~60–90 bytes of overhead before the value. In a system with 100 million
small keys, that is 6–9 GB just for key metadata. Use short key names in high-volume
systems (`u:1001:s` instead of `user:1001:sessions`).

### Memory sizing formula

```
estimated_memory = (number_of_keys × avg_key_overhead)
                 + (number_of_keys × avg_value_size)
                 + (replication_factor × above)
                 + 30% headroom for fork (AOF rewrite / RDB snapshot)

maxmemory = total_container_memory × 0.65
```

### Commands that kill performance at scale

| Dangerous command | Why | Safe alternative |
|---|---|---|
| `KEYS *` | O(N) over all keys, blocks server | `SCAN 0 COUNT 100` (paginated, non-blocking) |
| `SMEMBERS big_set` | Returns all members at once | `SSCAN` |
| `HGETALL big_hash` | Returns all fields at once | `HSCAN` or `HMGET` specific fields |
| `LRANGE list 0 -1` | Returns entire list | `LRANGE list 0 99` (paginated) |
| `ZRANGE big_zset 0 -1` | Returns all members | `ZRANGE with LIMIT` |
| `FLUSHDB` / `FLUSHALL` | Deletes everything, blocks | `FLUSHDB ASYNC` if you must |

### Monitoring metrics to watch

```
INFO memory          → used_memory, used_memory_rss, mem_fragmentation_ratio
INFO stats           → evicted_keys (should be 0 with noeviction)
INFO replication     → master_repl_offset, slave_repl_offset, repl_backlog_size
INFO keyspace        → key count per database
SLOWLOG GET 25       → commands that took > 10ms
LATENCY HISTORY      → latency spikes
```

**Alert thresholds:**
- `used_memory / maxmemory` > 70% → scale up or evict stale data
- `evicted_keys` > 0 → wrong eviction policy or under-provisioned
- `mem_fragmentation_ratio` > 1.5 → fragmentation is wasting memory (restart to defrag)
- `rdb_last_bgsave_status` = err → snapshot failing (memory pressure)
- `aof_last_bgrewrite_status` = err → AOF rewrite failing

---

## Part 6 — Industry Use Cases at Scale

### Twitter (now X) — 105TB RAM, 39M requests/second

**Problem:** Delivering tweets to followers in real time at scale. A celebrity with
100M followers tweeting creates 100M write operations.

**Solution:** Sorted Set timelines.
- Each user has a List of tweet IDs (their timeline cache)
- On tweet: fan-out writes to followers' Lists (LPUSH, LTRIM to 800)
- On read: LRANGE 0 799 from the user's List
- Inactive users (not seen in 30 days): timeline evicted; rebuilt from DB on next login
- 10,000 Redis instances, 30 billion updates per day

**Key lesson:** Redis handles the hot path (serving timelines to 300M active users).
The database handles persistence and inactive user data.

---

### GitHub — API rate limiting at scale

**Problem:** 5,000 requests per hour per authenticated user across hundreds of API servers.
The old solution (Memcached) competed for memory with other caches and had reliability issues.

**Solution:** Dedicated sharded Redis cluster for rate limiting only.
- Client-side sharding: each GitHub API server knows which Redis shard to talk to for each user
- One primary per shard + replicas for read failover
- Rate limit check: INCR + EXPIRE in a pipeline (fast path), fallback to database on Redis miss

**Key lesson:** Separate Redis instances by concern. Rate limiting Redis should never
compete for memory with session caching Redis.

---

### Uber — real-time driver location

**Problem:** Millions of drivers sending GPS updates every 4 seconds. Riders querying
"drivers within 500m" in under 100ms.

**Solution:** Sorted Sets as geographic indexes.
- The city is divided into H3 hexagonal cells
- Each cell has a Sorted Set: `drivers:{cell_id}` where score = last_update_timestamp
- Driver update: `ZADD drivers:{cell} {timestamp} {driver_id}`
- Nearby query: look up the cell the rider is in + 6 surrounding cells, ZRANGE all 7
- Zombie detection: `ZRANGEBYSCORE drivers:{cell} 0 {now-30}` → remove stale drivers

**Why not Redis GEO?** Redis GEO uses fixed geohash precision that didn't map to Uber's
H3 cells. Custom Sorted Set + H3 gave them control over cell size and precision.

**Key lesson:** Redis Sorted Sets can model geospatial problems. The score encodes any
numeric dimension — time, distance, priority.

---

### Pinterest — follower graph in Redis

**Problem:** 70M users, billions of follower relationships. Querying "does user A follow
user B?" must be sub-millisecond for feed generation.

**Solution:** Entire follower graph sharded in Redis.
- Each user's follower list: Set keyed by user ID, sharded across Redis instances by user ID
- `SISMEMBER user:{id}:followers {other_id}` → O(1) membership check
- `SCARD user:{id}:followers` → follower count
- AOF persistence for durability

**Key lesson:** Redis Sets for graph adjacency lists give O(1) edge existence checks.
At this scale, a database JOIN for every feed-generation decision would be milliseconds;
Redis is microseconds.

---

### Discord — presence system

**Problem:** 19M+ concurrent users. Showing online/idle/DND/offline status in real time
to potentially millions of people in large guilds.

**Solution:** Redis Cluster with Hash per user.
- `user:{id}:presence` Hash stores `status`, `last_seen`, `rich_presence_data`
- Updated via WebSocket heartbeats (every 45 seconds)
- Gateway pods subscribe to `guild:{id}:events` via Pub/Sub for real-time distribution
- Large guilds split across multiple gateway processes; each subscribes to the same channel

**Key lesson:** Pub/Sub works for fan-out events when fire-and-forget is acceptable
(a missed presence update is corrected by the next heartbeat). For guaranteed delivery,
use Streams.

---

### Stripe — idempotency keys

**Problem:** A network error causes the client to retry a payment. Must not charge twice.

**Solution:** Redis as idempotency store.
- Before processing: `SET idempotency:{key} "in_progress" NX EX 86400`
- If NX fails → another request with this key is in flight → return 409
- After processing: update to `"completed:{response_body}"` with a longer TTL
- On retry: key exists with completed response → return cached response

**Key lesson:** Redis `SET NX` (set-if-not-exists) is the foundation of distributed
idempotency. Two pods simultaneously processing the same request: only one can set
the NX key; the other gets nil and backs off.

---

### Stack Overflow — L1/L2 caching with Redis invalidation

**Problem:** Serve millions of page views with <50ms response time. Database queries
for the same data hit the DB thousands of times per second.

**Solution:** Two-level cache.
- L1: In-process memory cache per pod (microsecond access)
- L2: Redis (millisecond access, shared across pods)
- Invalidation: when data changes, publish `cache:invalidate:{type}:{id}` to Pub/Sub;
  all pods clear their L1 cache entries on receipt

**Key lesson:** Redis as L2 cache + Pub/Sub for invalidation eliminates the stale-data
problem of pure in-process caches without adding a database round-trip on every cache miss.

---

## Part 7 — Architect Decision Framework

### When to use Redis

| Use case | Why Redis | Alternative to consider |
|---|---|---|
| Session storage | Fast, TTL-native, shared across pods | Sticky sessions (don't scale) |
| Rate limiting | Atomic INCR, Lua for sliding window | In-process (not shared across pods) |
| Distributed lock | SET NX EX, safe with Lua release | ZooKeeper (stronger, more complex) |
| Pub/Sub notifications | Built-in, low latency | Kafka (if you need durability) |
| Leaderboard | Sorted Set, O(log N) rank | DB query (too slow at scale) |
| Geospatial proximity | Sorted Set or GEO commands | PostGIS (richer queries, slower) |
| Task queue | List or Stream | RabbitMQ (if you need routing) |
| Caching | O(1) access, TTL, rich eviction | Memcached (simpler, less featured) |
| Real-time counters | Atomic INCR, no race condition | DB row update (lock contention) |

### When NOT to use Redis

- **As a primary database.** Redis is in-memory; if the pod dies and persistence is
  misconfigured, you lose data. Use a persistent database (PostgreSQL, MSSQL) as the
  source of truth.
- **For data larger than available RAM.** Redis is limited to what fits in memory.
  For large datasets with some hot keys, use Redis for the hot subset only.
- **For complex queries.** Redis has no query language. If you need JOINs, aggregations,
  or complex filtering, use a database.
- **For blob storage.** Storing images or large documents in Redis wastes memory. Use
  object storage (S3, blob storage) and cache only metadata in Redis.

### Sentinel vs Cluster — the decision

```
Does your dataset fit on one machine (typically < 100GB)?
  YES → Use Sentinel
    Do you need read scaling (read replicas)?
      YES → Sentinel with replicas
      NO  → Single primary (simplest)

  NO → Use Redis Cluster
    Do you use multi-key operations?
      YES → Use hash tags to co-locate related keys on the same slot
      NO  → Cluster works without modification
```

### Common architectural mistakes

**1. Storing the full payload in Celery tasks.**
The task ID in Redis is a few bytes. The payload for a complex job can be kilobytes or
megabytes. With 10,000 queued tasks, that's gigabytes. Store only the ID; the worker
loads from the database.

**2. Using Redis as the only durability layer.**
"We persist to AOF" is not enough. AOF with `everysec` can lose up to 1 second of writes.
For anything that cannot be lost (payments, orders, requests), the database is source of
truth; Redis is a fast dispatch layer.

**3. Missing TTLs on cache keys.**
Keys accumulate. Memory fills. With `noeviction`, new writes start failing. With
`allkeys-lru`, task payloads start disappearing. Every cache key needs a TTL.

**4. KEYS * in production.**
O(N) over all keys, blocks the single-threaded Redis server for the duration. At 10M keys,
this can take seconds. Use `SCAN 0 MATCH pattern COUNT 100` — paginated, non-blocking.

**5. Not setting maxmemory.**
Default is unlimited. Redis grows until the container OOM-kills the pod. Set maxmemory
to 65% of container memory. Leave headroom for fork-time copy-on-write.

**6. Hot key problem.**
If one Redis key receives millions of requests per second (celebrity user, viral tweet),
a single Redis instance handles all of it (single-threaded). Solutions: local in-process
caching for the hot key, read replicas with client-side load balancing, or key sharding
(store `hot_key:shard:{0..N}` and aggregate).

**7. Large key problem.**
A List with 10M entries, a Hash with 100K fields, a Set with 1M members. Any `LRANGE 0 -1`
or `SMEMBERS` blocks the server for the time to send all data. Monitor key sizes with
`MEMORY USAGE key` and `redis-cli --bigkeys`.

---

## Part 8 — Study Plan

Start: 2026-08-18

### Week 1 — Foundations (2 hours/day)

**Day 1 (Mon):** Data structures — String, Hash, List
- Install Redis locally (`docker run -p 6379:6379 redis`)
- Open `redis-cli` and type every command in Part 1 for these three types
- Goal: know when to reach for each one

**Day 2 (Tue):** Data structures — Set, Sorted Set
- Practice leaderboard with ZADD, ZRANGE, ZREVRANK, ZINCRBY
- Practice friend intersection with SINTER
- Build a mini rate limiter with Sorted Set (the sliding window pattern from Part 3)

**Day 3 (Wed):** TTL, eviction policies, persistence
- Set up two Redis instances: one with `allkeys-lru`, one with `noeviction`
- Fill both to maxmemory and observe what happens on the next write
- Configure AOF and RDB, restart Redis, observe data survives
- Read the redis.conf in this project and understand every line

**Day 4 (Thu):** Pub/Sub and Streams
- Subscribe in one terminal, publish from another — see messages arrive
- Create a Stream, add events, create a consumer group, process with two consumers
- Understand: when would you use Pub/Sub vs Streams vs a List queue?

**Day 5 (Fri):** Lua scripting
- Implement the token bucket Lua script from this project in a clean Redis
- Run it from Python with `redis.eval()`
- Break it intentionally (run two clients simultaneously without Lua) to see the race

**Weekend:** Read the `token_bucket.lua` and `semaphore_acquire.lua` in this project.
Understand every line.

---

### Week 2 — Intermediate (2 hours/day)

**Day 6 (Mon):** Transactions vs Lua
- Implement the same operation two ways: MULTI/EXEC and Lua
- Understand which one allows conditional logic and why
- Implement the distributed lock pattern from Part 3

**Day 7 (Tue):** Memory management
- Run `INFO memory` and understand every field
- Run `redis-cli --bigkeys` on a populated Redis
- Run `redis-cli --hotkeys` (requires `maxmemory-policy allkeys-lfu`)
- Calculate memory for a hypothetical 1M-user system using the formula in Part 5

**Day 8 (Wed):** Sentinel setup
- Run 1 primary + 2 replicas + 3 Sentinels using Docker Compose
- Kill the primary, watch Sentinel promote a replica
- Connect a Python client through Sentinel (not directly to primary)

**Day 9 (Thu):** Redis Cluster setup
- Set up a 3-shard cluster (6 nodes: 3 primary + 3 replica) with Docker
- Observe where keys land with `CLUSTER KEYSLOT mykey`
- Use hash tags: `{user:1001}:sessions` vs `user:1001:sessions`
- Try a multi-key operation across slots — see the CROSSSLOT error

**Day 10 (Fri):** Monitoring and alerting
- Set `slowlog-log-slower-than 1000` (1ms) and run slow commands
- Read SLOWLOG output
- Set up a simple alert: poll `used_memory / maxmemory` every 30 seconds

**Weekend:** Read the GitHub blog post on their sharded rate limiter.
Read the Twitter Redis architecture article (links in Part 6). Make notes.

---

### Week 3 — Advanced patterns (2 hours/day)

**Day 11 (Mon):** Build a complete rate limiter
- Implement sliding window rate limiter from Part 3 in Python
- Test it with 10 concurrent clients hitting the same user key
- Verify no count is ever skipped or doubled

**Day 12 (Tue):** Build a counting semaphore
- Implement the semaphore from Part 3 with acquire, release, renew
- Test with 5 concurrent workers, max_slots = 3
- Kill a worker mid-hold, verify the slot is reclaimed via TTL

**Day 13 (Wed):** Build a circuit breaker
- Implement a simple 3-state circuit breaker stored in Redis
- Wire it to a mock HTTP client that fails 50% of the time
- Verify the breaker opens, blocks traffic, and recovers via half-open probe

**Day 14 (Thu):** Geospatial — build a driver-location system
- Add 100 mock driver positions with GEOADD
- Query "drivers within 1km" with GEOSEARCH
- Implement Uber's zombie detection: ZRANGEBYSCORE to find drivers who haven't updated in 30s

**Day 15 (Fri):** Streams — build a task queue with guaranteed delivery
- Producer writes jobs to a Stream
- Two consumers in a consumer group, each acknowledging after processing
- Kill one consumer mid-processing, verify the pending job is re-delivered

**Weekend:** Design review. Take any one system you've built at work and write down
how you would add Redis to it. What data structure? What coordination pattern?
What eviction policy? What persistence?

---

### Week 4 — Architect mindset

**Day 16 (Mon):** Read the F5 project's coordination layer fully
- `app/coordination/semaphore.py` + Lua scripts
- `app/coordination/ratelimit.py` + Lua
- `app/coordination/breaker.py` + Lua
- For each: why this data structure, why Lua, what happens if Redis is down

**Day 17 (Tue):** Failure modes
- What happens when Redis goes down with `noeviction`?
- What happens with `allkeys-lru`?
- How does Sentinel failover affect Lua scripts in flight?
- What happens to a circuit breaker's state during a Redis restart?

**Day 18 (Wed):** Anti-patterns
- Find a codebase (any project) that uses Redis
- Identify: missing TTLs, KEYS * usage, payload-in-queue, missing maxmemory
- Write a short analysis of what would break under load

**Day 19 (Thu):** Architecture decisions exercise
- Design Redis architecture for: a ride-hailing app, a banking API rate limiter,
  a social network feed
- For each: data structures, key design, eviction policy, Sentinel vs Cluster, persistence

**Day 20 (Fri):** Review and consolidate
- Go back to Week 1 code — does it make more sense now?
- Write a 1-page "when I reach for Redis and what I reach for" cheat sheet for yourself

---

### Resources

| Resource | What it covers | When to read |
|---|---|---|
| redis.io/docs/latest/develop/data-types/ | Official data type reference | Week 1 |
| *Redis in Action* — Josiah Carlson | Redis as a platform, not just a cache | Week 1–2 |
| github.blog — "How we scaled GitHub API" | Real sharded rate limiter in production | Week 2 weekend |
| highscalability.com — Twitter Redis post | 105TB, 39M QPS architecture | Week 2 weekend |
| redis.io/docs/latest/develop/patterns/ | Pub/Sub patterns, distributed locks | Week 3 |
| redis.io/topics/cluster-tutorial | Cluster architecture and hash slots | Week 2 Day 9 |
| redis.io/topics/sentinel | Sentinel failover mechanics | Week 2 Day 8 |
| Lua 5.1 reference (only need 5 pages) | The subset Redis uses | Week 1 Day 5 |
| redis.io/tutorials/redis-anti-patterns | What not to do | Week 4 Day 18 |

---

### What "Redis architect" means in practice

You are a Redis architect when you can answer these without thinking:

1. A new microservice needs rate limiting. What data structure, what Lua script, what eviction policy?
2. Two pods are processing the same job. How do you prevent it with Redis?
3. Redis OOM killed your pod at 3am. What three things do you check first?
4. You need geospatial proximity queries. Do you use Redis GEO or a Sorted Set? Why?
5. Your hot key is receiving 500K requests/second. Redis is single-threaded. What do you do?
6. A developer says "let's store user sessions in Redis with noeviction." What is the risk?
7. When do you choose Cluster over Sentinel?
8. Your Lua script works in development but breaks in Redis Cluster production. Why?
9. You need to queue 10,000 jobs. Should the queue entry contain the job payload?
10. How do you safely release a distributed lock without accidentally releasing another pod's lock?

If you can answer all 10 confidently, you are thinking at the architect level.
