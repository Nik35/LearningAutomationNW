# Redis — Complete Architect's Guide
## Basics to Production-Grade Distributed Systems

**Study starts:** 2026-08-18
**Study plan:** Part 11 at the end

---

## The mental model that makes everything else click

Redis is a **single-threaded, in-memory state machine with a network interface and a
Lua interpreter built in**.

Every command — from every client, every pod, every datacenter — executes one at a time
on a single thread inside Redis. There is no lock contention. No deadlocks. No parallel
execution. This is why:

1. Redis is fast — no context switching between threads, no lock overhead
2. INCR is race-condition-free — it is literally impossible for two INCRs to collide
3. Lua scripts give you true atomicity — nothing else runs while a Lua script executes
4. One slow command (like `KEYS *`) blocks every client in the world simultaneously

Everything in this guide follows from this model. When you are confused about Redis
behaviour, return to this model and ask: "what does a single-threaded state machine do here?"

---

## Part 1 — Data Structures

Redis is not a key-value store. It is a data structure server. Choosing the wrong structure
is the most common source of performance problems in Redis.

### 1.1 String

The atomic unit. Stores bytes — can be text, JSON, numbers, binary. Max 512MB per value.

**Commands:**
```
SET    key value [EX seconds] [PX ms] [NX|XX]
GET    key
MSET   k1 v1 k2 v2 k3 v3          → set multiple (atomic)
MGET   k1 k2 k3                    → get multiple (one round trip)
INCR   key                         → atomic +1, returns new value
INCRBY key 10                      → atomic +N
INCRBYFLOAT key 0.5                → atomic float increment
APPEND key " more text"            → append to string
STRLEN key                         → length in bytes
GETSET key newvalue                → atomic get-then-set (deprecated, use GETDEL+SET)
GETDEL key                         → get and delete atomically
SET    key value NX EX 30         → set only if not exists, expire in 30s (lock pattern)
SET    key value XX                → set only if already exists (update pattern)
```

**What it looks like in practice:**
```
127.0.0.1:6379> SET visits:page:home 0
OK
127.0.0.1:6379> INCR visits:page:home
(integer) 1
127.0.0.1:6379> INCR visits:page:home
(integer) 2
127.0.0.1:6379> INCRBY visits:page:home 100
(integer) 102
127.0.0.1:6379> SET session:abc123 '{"user_id":1001,"role":"admin"}' EX 3600
OK
127.0.0.1:6379> TTL session:abc123
(integer) 3598
```

**Internal encoding:** Strings under 44 bytes are stored as `embstr` (one allocation).
Longer strings use `raw` (two allocations). Numbers are stored as integers if they fit
in a 64-bit int — this is why INCR is O(1) and atomic.

**Memory:** ~56 bytes overhead + key size + value size. A million counters ≈ 56MB overhead.

**When to use:** Caching any single value, counters, rate limit counts, session tokens,
distributed locks, idempotency keys, feature flags, JSON blobs.

---

### 1.2 Hash

A map of field→value pairs stored under one key. The most natural structure for objects.

**Commands:**
```
HSET   user:1001 name "Nikhil" role "architect" city "Bangalore"
HGET   user:1001 name                   → "Nikhil"
HMGET  user:1001 name role              → ["Nikhil", "architect"]
HGETALL user:1001                       → all field-value pairs
HKEYS  user:1001                        → ["name", "role", "city"]
HVALS  user:1001                        → ["Nikhil", "architect", "Bangalore"]
HLEN   user:1001                        → 3   (O(1))
HEXISTS user:1001 name                  → 1
HDEL   user:1001 city                   → 1
HINCRBY  user:1001 login_count 1        → atomic increment on one field
HINCRBYFLOAT user:1001 score 0.5
HSCAN  user:1001 0 COUNT 10            → paginated field iteration
```

**What it looks like in practice:**
```
127.0.0.1:6379> HSET device:f5-dc-a slots_used 3 last_seen 1724000000
(integer) 2
127.0.0.1:6379> HINCRBY device:f5-dc-a slots_used 1
(integer) 4
127.0.0.1:6379> HGETALL device:f5-dc-a
1) "slots_used"
2) "4"
3) "last_seen"
4) "1724000000"
```

**Internal encoding:** Redis uses `listpack` (compact array) for hashes with fewer than
128 fields where each value is under 64 bytes. Above either threshold, it switches to a
true `hashtable`. The listpack is 3–4× more memory efficient. This matters: if you store
user data with many small fields, keep the hash small and Redis compresses it for free.

**Memory:** A hash with 10 small fields costs ~100–200 bytes in listpack encoding vs
~500–700 bytes in hashtable encoding.

**When to use:** Object with multiple properties (user profile, device config, session
data), grouping related counters per entity, per-worker slot tracking (semaphore — field =
worker_id, value = timestamp).

**Architect caution:** `HGETALL` on a hash with 10,000 fields blocks the server and returns
10,000 items to the client. Use `HSCAN` for large hashes. In the semaphore pattern, `HLEN`
is O(1) regardless of size — that's why we use a Hash for the semaphore, not a counter.

---

### 1.3 List

Doubly-linked list. O(1) push/pop from either end. O(N) access by index.

**Commands:**
```
RPUSH  queue "job:1" "job:2" "job:3"    → push to tail
LPUSH  queue "urgent"                    → push to head
LPOP   queue                             → pop from head (FIFO)
RPOP   queue                             → pop from tail (LIFO / stack)
LLEN   queue                             → length
LRANGE queue 0 9                         → first 10 items (does NOT remove)
LINDEX queue 0                           → get item at index 0
LREM   queue 1 "job:1"                  → remove 1 occurrence of "job:1"
LTRIM  queue 0 999                       → keep only first 1000 items (trim)
BRPOP  queue other_queue 30             → blocking pop, wait up to 30s
LMOVE  source destination LEFT RIGHT    → atomic move between lists (reliable queue)
```

**Reliable queue pattern with LMOVE:**
```
# Producer
RPUSH jobs:pending "job:42"

# Worker — atomic claim (no job lost if worker crashes)
job = LMOVE jobs:pending jobs:processing LEFT RIGHT

# After successful completion
LREM jobs:processing 1 "job:42"

# Recovery sweeper — jobs stuck in processing
LRANGE jobs:processing 0 -1   → check timestamps, re-queue stalled jobs
```

**Twitter timeline example:**
```
# When Nikhil posts a tweet (tweet_id = 9999)
LPUSH timeline:follower:1001 "9999"
LPUSH timeline:follower:1002 "9999"
LTRIM timeline:follower:1001 0 799   → cap at 800 entries

# When follower opens their feed
LRANGE timeline:follower:1001 0 49  → 50 most recent tweet IDs
# Fetch the actual tweet content from DB in one batched query
```

**Memory:** Each list node costs ~40 bytes + value. Compact `listpack` encoding is used
for lists with fewer than 128 elements where each element is under 64 bytes.

**When to use:** Task queues, activity feeds (bounded to last N events), message passing
between processes, undo/redo stacks, log buffers.

**When NOT to use:** If you need to find "does X exist in this list?" — that's O(N).
Use a Set for membership checks.

---

### 1.4 Set

Unordered collection of unique strings. O(1) add, remove, and membership check.

**Commands:**
```
SADD   tags:article:42 "redis" "distributed" "architecture"
SREM   tags:article:42 "architecture"
SISMEMBER tags:article:42 "redis"          → 1 (exists)
SMEMBERS  tags:article:42                  → all members (avoid on large sets)
SCARD     tags:article:42                  → count (O(1))
SSCAN     tags:article:42 0 COUNT 10       → paginated iteration

# Set operations (great for social graphs)
SINTER user:1001:following user:1002:following   → mutual follows
SUNION user:1001:following user:1002:following   → all people either follows
SDIFF  user:1001:following user:1002:following   → who 1001 follows but 1002 doesn't
SMOVE  source:set dest:set "member"              → atomic move between sets
SRANDMEMBER tags:article:42 3                    → 3 random members (sampling)
SPOP   tags:article:42                           → remove and return random member
```

**Deduplication example:**
```
# Track unique visitors per page per day
SADD visitors:home:2026-08-18 "user:1001"
SADD visitors:home:2026-08-18 "user:1001"  → still only 1 member
SADD visitors:home:2026-08-18 "user:1002"
SCARD visitors:home:2026-08-18              → 2 (correct unique count)

# Find people who visited both pages (cross-sell opportunity)
SINTER visitors:home:2026-08-18 visitors:pricing:2026-08-18
```

**Internal encoding:** `intset` for sets containing only integers (extremely compact —
integers stored in a sorted array, ~8 bytes each). `listpack` for small sets of strings.
`hashtable` when either threshold (128 members, 64 bytes per member) is exceeded.

**Memory:** intset with 1000 integers ≈ 8KB. listpack with 100 short strings ≈ 5–10KB.
hashtable: much higher, ~100 bytes per member.

**When to use:** Membership checks, unique visitor tracking, tagging, permission sets,
friend/follower graphs, deduplication.

---

### 1.5 Sorted Set (ZSet)

The most powerful Redis data structure. Like a Set but every member has a float score.
Members are always ordered by score. All score-order operations are O(log N).

**Commands:**
```
ZADD   leaderboard 9850.0 "alice" 9200.0 "bob" 8100.0 "carol"
ZADD   leaderboard NX 7500.0 "dave"          → add only if not exists
ZADD   leaderboard XX GT 9900.0 "alice"      → update only if new score is greater
ZINCRBY leaderboard 150 "bob"                → atomic score increment → 9350.0
ZSCORE  leaderboard "bob"                    → 9350.0
ZRANK   leaderboard "bob"                    → 1  (0-indexed, ascending)
ZREVRANK leaderboard "bob"                   → 1  (rank from top)
ZCARD   leaderboard                          → 4
ZRANGE  leaderboard 0 2 WITHSCORES          → bottom 3 with scores
ZREVRANGE leaderboard 0 2 WITHSCORES        → top 3 with scores
ZRANGEBYSCORE leaderboard 9000 +inf         → all with score >= 9000
ZRANGEBYSCORE leaderboard -inf +inf LIMIT 0 10  → pagination
ZCOUNT  leaderboard 9000 +inf               → count with score >= 9000
ZREM    leaderboard "carol"
ZREMRANGEBYSCORE leaderboard -inf 8000      → remove all with score < 8000
ZPOPMAX leaderboard 3                       → remove and return top 3
ZUNIONSTORE dest 2 zset1 zset2 WEIGHTS 1 0.5  → weighted union
```

**Rate limiter with Sorted Set (sliding window):**
```
# KEYS[1]=rate_limit:{user_id}, ARGV[1]=limit, ARGV[2]=window_sec, ARGV[3]=now
local now    = tonumber(ARGV[3])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[1])
local cutoff = now - window

-- remove requests older than the window
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)

-- count requests in the window
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
    -- unique member = now + random to handle same-millisecond requests
    redis.call('ZADD', KEYS[1], now, now .. ':' .. math.random(1000000))
    redis.call('EXPIRE', KEYS[1], window)
    return 1  -- allowed
end
return 0  -- rejected
```

**Priority queue:**
```
ZADD priority_queue 1 "low_priority_job"
ZADD priority_queue 10 "high_priority_job"
ZPOPMAX priority_queue   → always returns highest priority
```

**Leaderboard with ties (same score = lexicographic order):**
```
ZADD scores 100 "alice:1001"
ZADD scores 100 "bob:1002"
ZREVRANGE scores 0 -1 WITHSCORES   → alice and bob both at 100, ordered by name
```

**Internal encoding:** `listpack` for small sorted sets (under 128 members, each under
64 bytes). Above threshold: a combination of **skip list** (for ordered traversal,
O(log N)) + **hash table** (for O(1) score lookup by member).

**When to use:** Leaderboards, priority queues, rate limiting (sliding window), timeline
ordering by timestamp, geospatial (score = encoded lat/lon), delayed job scheduling
(score = execute_at unix timestamp).

---

### 1.6 Stream

Append-only log with consumer groups. The modern Redis queue with guaranteed delivery.
Think of it as a lightweight Kafka built into Redis.

**Commands:**
```
# Producer
XADD orders:stream * action "created" order_id "9001" amount "250.00"
  → returns message ID like "1724000000000-0"

XADD orders:stream MAXLEN ~ 100000 * action "created" ...
  → auto-trim to ~100K entries (approximate for performance)

# Consumer — basic
XREAD COUNT 10 BLOCK 5000 STREAMS orders:stream 0-0    → read from beginning
XREAD COUNT 10 BLOCK 5000 STREAMS orders:stream $      → only new messages

# Consumer groups (multiple workers, each gets different messages)
XGROUP CREATE orders:stream workers $ MKSTREAM
XREADGROUP GROUP workers worker-pod-a COUNT 5 BLOCK 5000 STREAMS orders:stream >
  → ">" means: give me messages not yet delivered to any consumer in this group

# Acknowledge (marks as processed)
XACK orders:stream workers "1724000000000-0"

# Check unacknowledged (pending) messages
XPENDING orders:stream workers - + 10
  → shows messages delivered but not yet acked

# Claim stuck messages (if a worker crashed)
XCLAIM orders:stream workers worker-pod-b 60000 "1724000000000-0"
  → claim a message idle for more than 60000ms

# Stream info
XLEN orders:stream          → total messages
XINFO STREAM orders:stream  → details
XINFO GROUPS orders:stream  → consumer group details
```

**At-least-once delivery flow:**
```
Worker starts → XREADGROUP gets message → processes it → XACK
If worker crashes: message stays in PEL (pending entry list)
Recovery sweeper: XPENDING to find old messages → XCLAIM to reassign → reprocess
```

**When to use:** Event sourcing, audit trails, async task processing where delivery
guarantee matters, replacing a List-based queue in new projects. Choose Streams over
Lists for new queues — Streams have acknowledgement, consumer groups, and replay.

---

### 1.7 HyperLogLog

Probabilistic unique counter. Uses ~12KB regardless of dataset size. ~0.81% error margin.
Does NOT store actual values — cannot list members, only count them.

```
PFADD  daily:active:2026-08-18 "user:1001" "user:1002" "user:1001"
PFCOUNT daily:active:2026-08-18     → 2 (deduplicated, approximate)
PFMERGE week:active daily:active:2026-08-18 daily:active:2026-08-19
PFCOUNT week:active                  → approximate unique users in both days
```

**Real scale example:** 100M daily active users tracked per day.
- Storing actual user IDs: 100M × 8 bytes = 800MB per day
- HyperLogLog: 12KB per day, ~0.81% error

**When to use:** Counting distinct values at scale where approximate is acceptable:
unique visitors, unique search queries, distinct items seen.

**When NOT to use:** When you need the exact count (use a Set) or when you need to
retrieve individual members.

---

### 1.8 Bitmap

A String interpreted as a sequence of bits. Extremely memory-efficient for per-user flags.

```
SETBIT  user:logins:2026-08-18 1001 1    → user 1001 logged in today
GETBIT  user:logins:2026-08-18 1001      → 1
BITCOUNT user:logins:2026-08-18          → total logins today
BITCOUNT user:logins:2026-08-18 0 99     → logins for user IDs 0-799 (bytes 0-99)

# Who was active ALL 5 days this week? (AND operation)
BITOP AND active:all:week \
  user:logins:mon user:logins:tue user:logins:wed user:logins:thu user:logins:fri
BITCOUNT active:all:week    → count of users active every day this week

# Streak tracking
SETBIT user:1001:streaks 0 1   → active on day 0
SETBIT user:1001:streaks 1 1   → active on day 1
SETBIT user:1001:streaks 2 0   → missed day 2
BITCOUNT user:1001:streaks     → total active days
```

**Memory:** 10M users tracked per day = 10M bits = 1.25MB. 365 days = 456MB.
Compared to a Set: 10M × 50 bytes (UUID strings) = 500MB for ONE day.

**When to use:** Daily active users, feature flag rollout by user ID, attendance tracking,
permission bitmaps, A/B test cohort membership.

---

### 1.9 Geo

Stores lat/lon pairs. Internally a Sorted Set with scores encoded as geohashes.

```
GEOADD fleet:drivers 77.5946 12.9716 "driver:001"   → (longitude, latitude, member)
GEOADD fleet:drivers 77.6101 12.9352 "driver:002"
GEODIST fleet:drivers "driver:001" "driver:002" km   → distance in km
GEOPOS  fleet:drivers "driver:001"                   → [77.5946, 12.9716]

# Find nearby drivers within 2km, return closest first
GEOSEARCH fleet:drivers FROMMEMBER "driver:001" BYRADIUS 2 km ASC
  COUNT 10 WITHCOORD WITHDIST

# Or from a lat/lon position (rider's location)
GEOSEARCH fleet:drivers FROMLONLAT 77.5990 12.9700 BYRADIUS 2 km ASC
```

**Precision:** Geohash encoding has ~0.6m precision at 52-bit (Redis default).
Sufficient for "nearby" queries; not for turn-by-turn navigation.

**When to use:** Driver/delivery proximity, store locator, geo-fencing, location-aware
rate limiting ("more than 5 login attempts from different countries in 1 hour").

---

## Part 2 — Caching Patterns (critical for architects)

This section covers HOW to cache correctly, not just that Redis can cache.

### 2.1 Cache-Aside (Lazy Loading)

The most common pattern. Application manages the cache explicitly.

```python
def get_user(user_id: int) -> dict:
    # 1. Check cache
    cached = redis.get(f"user:{user_id}")
    if cached:
        return json.loads(cached)

    # 2. Cache miss — load from DB
    user = db.query("SELECT * FROM users WHERE id = ?", [user_id])

    # 3. Write to cache with TTL
    redis.setex(f"user:{user_id}", 3600, json.dumps(user))

    return user
```

**Pros:** Cache only contains what is actually requested. Cold start is gradual.
**Cons:** First request after a miss is slow. Cache can get stale if DB changes.
**Staleness fix:** Short TTL, or explicit invalidation on write.

---

### 2.2 Write-Through

Write to cache AND database simultaneously on every update.

```python
def update_user(user_id: int, data: dict):
    # 1. Write to database
    db.execute("UPDATE users SET ... WHERE id = ?", [user_id])

    # 2. Write to cache immediately (always fresh)
    redis.setex(f"user:{user_id}", 3600, json.dumps(data))
```

**Pros:** Cache is always consistent with the database. No stale reads.
**Cons:** Write latency increases (must write to both). Cache fills with data that
may never be read. Works well for data that is read frequently after write.

---

### 2.3 Write-Behind (Write-Back)

Write to cache immediately; flush to database asynchronously.

```python
def update_user(user_id: int, data: dict):
    # 1. Write to cache immediately (fast)
    redis.setex(f"user:{user_id}", 3600, json.dumps(data))

    # 2. Queue DB write for async processing
    redis.rpush("db:write:queue", json.dumps({
        "table": "users",
        "id": user_id,
        "data": data
    }))
    # Background worker drains this queue and writes to DB
```

**Pros:** Write latency is very low (only Redis).
**Cons:** Risk of data loss if Redis dies before the DB write. Complex recovery.
Use only when write throughput matters more than durability (e.g., analytics counters,
view counts — losing a few counts on crash is acceptable).

---

### 2.4 Cache Stampede (Thundering Herd) — Critical architect knowledge

**The problem:** A hot cache key expires. Simultaneously, 500 requests all get a cache
miss and all query the database at once. The database gets 500 queries for the same data.

```
t=0: 500 requests hit cache → all miss → all hit DB → DB falls over
```

**Solution 1 — Probabilistic early expiration:**
```python
import random
import time

def get_with_jitter(key: str, ttl: int, fetch_fn):
    cached = redis.get(key)
    if cached:
        data, expires_at = json.loads(cached)
        remaining = expires_at - time.time()
        # Start refreshing randomly when less than 10% of TTL remains
        if remaining < ttl * 0.1 and random.random() < 0.1:
            cached = None  # force refresh for this request only

    if not cached:
        data = fetch_fn()
        expires_at = time.time() + ttl
        redis.setex(key, ttl, json.dumps([data, expires_at]))

    return data
```

**Solution 2 — Mutex lock (one requester refreshes, others wait):**
```python
import time

def get_with_lock(key: str, ttl: int, fetch_fn):
    cached = redis.get(key)
    if cached:
        return json.loads(cached)

    lock_key = f"lock:{key}"
    # Try to acquire the refresh lock
    acquired = redis.set(lock_key, "1", nx=True, ex=5)

    if acquired:
        # I got the lock — I refresh
        try:
            data = fetch_fn()
            redis.setex(key, ttl, json.dumps(data))
            return data
        finally:
            redis.delete(lock_key)
    else:
        # Another pod is refreshing — wait briefly and try again
        time.sleep(0.05)
        return get_with_lock(key, ttl, fetch_fn)
```

**Solution 3 — Stale-while-revalidate:**
```python
def get_stale_ok(key: str, ttl: int, soft_ttl: int, fetch_fn):
    """
    soft_ttl: serve stale data for up to this long while refreshing async
    ttl: hard expiry
    """
    cached = redis.get(key)
    if cached:
        data, created_at = json.loads(cached)
        age = time.time() - created_at

        if age > soft_ttl:
            # Trigger async background refresh but serve stale data now
            asyncio.create_task(refresh_cache(key, ttl, fetch_fn))

        return data  # serve stale data immediately

    # Hard miss — must fetch synchronously
    data = fetch_fn()
    redis.setex(key, ttl, json.dumps([data, time.time()]))
    return data
```

**Which to use:**
- High-traffic API, DB can handle brief spikes → probabilistic expiry
- DB cannot handle concurrent spikes → mutex lock
- Latency is critical, slight staleness OK → stale-while-revalidate

---

### 2.5 Cache Invalidation

The hardest problem in distributed caching. Two approaches:

**TTL-based (simple):** Cache expires after N seconds. Stale for up to N seconds.
Good for data that changes infrequently or where slight staleness is acceptable.

**Event-based invalidation (strong consistency):**
```python
# On every DB write:
def update_user(user_id: int, data: dict):
    db.execute("UPDATE users ...", [data, user_id])
    redis.delete(f"user:{user_id}")               # invalidate in own pod
    redis.publish("cache:invalidate:user", str(user_id))  # tell other pods

# Every pod subscribes at startup:
async def subscribe_to_invalidations():
    pubsub = redis.pubsub()
    await pubsub.subscribe("cache:invalidate:user")
    async for msg in pubsub.listen():
        if msg["type"] == "message":
            user_id = msg["data"]
            local_cache.delete(f"user:{user_id}")  # clear in-process L1 cache
```

---

### 2.6 Two-Level Caching (L1 + L2)

Used by Stack Overflow, GitHub, Discord.

```
Request → L1 (in-process dict, microseconds) → L2 (Redis, milliseconds) → DB (milliseconds-seconds)
```

```python
import cachetools  # in-process LRU cache

L1 = cachetools.TTLCache(maxsize=10000, ttl=60)   # 60s in-process cache

async def get_user(user_id: int) -> dict:
    # L1 hit
    if user_id in L1:
        return L1[user_id]

    # L2 hit
    cached = await redis.get(f"user:{user_id}")
    if cached:
        user = json.loads(cached)
        L1[user_id] = user
        return user

    # DB hit
    user = await db.fetch_user(user_id)
    await redis.setex(f"user:{user_id}", 300, json.dumps(user))
    L1[user_id] = user
    return user
```

**Invalidation for two-level cache:** Delete from L2 + publish invalidation → all pods
clear their L1 on receipt. L1 also self-expires via TTL as a safety net.

---

## Part 3 — Advanced Redis Features

### 3.1 Pipelining

Without pipelining, every command is a round trip:
```
Client → SEND command → Server → PROCESS → SEND result → Client
~1ms per round trip × 1000 commands = 1 second
```

With pipelining, batch multiple commands in one network round trip:
```python
# Without pipeline: 1000 round trips
for user_id in user_ids:
    redis.get(f"user:{user_id}")    # 1000 separate round trips

# With pipeline: 1 round trip
pipe = redis.pipeline()
for user_id in user_ids:
    pipe.get(f"user:{user_id}")     # queued locally
results = pipe.execute()            # sent in one batch, all results returned at once
```

**Rules for pipelines:**
- Commands in a pipeline are NOT atomic — another client can interleave between them
- Use pipelines for performance; use Lua for atomicity
- Pipeline batches of 100–1000 commands; very large pipelines consume memory on both ends
- Works beautifully for bulk reads (hydrate 100 IDs in one round trip)

---

### 3.2 WATCH / Optimistic Locking

Check-and-set without Lua. Watches a key; if it changes before EXEC, the transaction aborts.

```python
def transfer_tokens(from_user: str, to_user: str, amount: int):
    with redis.pipeline() as pipe:
        while True:
            try:
                pipe.watch(f"balance:{from_user}")
                balance = int(pipe.get(f"balance:{from_user}") or 0)

                if balance < amount:
                    raise ValueError("Insufficient balance")

                pipe.multi()   # start transaction
                pipe.decrby(f"balance:{from_user}", amount)
                pipe.incrby(f"balance:{to_user}", amount)
                pipe.execute()
                break          # success

            except redis.WatchError:
                continue       # another client modified the key — retry
```

**When to use WATCH vs Lua:**
- WATCH: when the transaction is simple and retries are cheap
- Lua: when you need conditional logic inside the atomic block, or retries are expensive
- Lua is almost always the better choice for new code

---

### 3.3 Keyspace Notifications

Redis publishes events when keys expire, are deleted, or are modified. Subscribe to be
notified when a distributed lock expires, a session times out, or a cache key is deleted.

```
# redis.conf — enable expired key notifications
notify-keyspace-events "Ex"
# K = keyspace events, x = expired events, d = del events, g = generic (del, expire, etc.)
```

```python
# Subscribe to expiry events for a specific key pattern
pubsub = redis.pubsub()
await pubsub.psubscribe("__keyevent@0__:expired")

async for message in pubsub.listen():
    if message["type"] == "pmessage":
        expired_key = message["data"]
        if expired_key.startswith("lock:"):
            resource = expired_key.removeprefix("lock:")
            log.warning("lock_expired_without_release", resource=resource)
            # Trigger recovery — this lock was held by a crashed process
```

**Real use case:** The reclaim sweeper in this F5 project uses `last_heartbeat_at` in
the database to detect dead workers. An alternative design: set a key `alive:{worker_id}`
with TTL = heartbeat interval. When the worker dies, the key expires. A keyspace
notification subscriber detects the expiry and triggers reclaim immediately — no polling.

---

### 3.4 Lua script caching with EVALSHA

Loading a Lua script every time wastes bandwidth. Cache the script:

```python
# First time: load the script, get its SHA1
sha = redis.script_load(LUA_TOKEN_BUCKET_SCRIPT)   # returns SHA1 hash

# All subsequent calls: use the SHA1 (much faster, no script transfer)
result = redis.evalsha(sha, 1, key, capacity, refill_rate, 1, time.time())
```

Redis caches scripts by SHA1. If Redis restarts, the cache is cleared — your code must
handle `NOSCRIPT` errors by re-loading the script and retrying.

```python
try:
    result = redis.evalsha(sha, 1, key, ...)
except redis.exceptions.NoScriptError:
    sha = redis.script_load(script)  # reload
    result = redis.evalsha(sha, 1, key, ...)
```

---

### 3.5 Redis Modules (modern Redis)

Redis 4.0+ supports loadable modules that add new data types and commands.

**RedisJSON:** Store and query native JSON documents.
```
JSON.SET  product:1001 $ '{"name":"widget","price":29.99,"tags":["sale","new"]}'
JSON.GET  product:1001 $.name              → "widget"
JSON.GET  product:1001 $.price             → 29.99
JSON.NUMINCRBY product:1001 $.price 5     → price becomes 34.99
JSON.ARRAPPEND product:1001 $.tags '"trending"'
```

**RediSearch:** Full-text search and secondary indexes on Redis data.
```
FT.CREATE idx:products ON JSON SCHEMA $.name TEXT $.price NUMERIC
FT.SEARCH idx:products "widget" FILTER price 20 50
  → returns matching products with full documents
```

**RedisBloom:** Bloom filter, Cuckoo filter, Count-Min Sketch, Top-K.
```
BF.ADD  seen:urls "https://example.com/page"
BF.EXISTS seen:urls "https://example.com/page"   → 1 (probably yes, ~0.01% false positive)
```
Use: "have I seen this URL before?" at web-crawler scale. Stores billions of URLs in
gigabytes instead of terabytes.

**RedisTimeSeries:** Optimised time-series data.
```
TS.CREATE device:f5-dc-a:latency_ms RETENTION 86400000
TS.ADD    device:f5-dc-a:latency_ms * 145.3    → * = use current timestamp
TS.RANGE  device:f5-dc-a:latency_ms - + AGGREGATION avg 60000  → 1-min averages
```
Use: storing Prometheus-style metrics directly in Redis.

---

### 3.6 Key Design Patterns

Consistent key naming prevents collisions and makes debugging possible.

```
# Pattern: namespace:entity_type:entity_id:field
user:session:abc123
user:1001:profile
device:f5-dc-a:sem
device:f5-dc-a:bucket
device:f5-dc-a:breaker:state
queue_depth:global
queue_depth:f5-dc-a

# For cluster sharding — use hash tags to co-locate related keys
{user:1001}:profile       # hash tag = "user:1001"
{user:1001}:sessions      # same hash tag = same shard
{user:1001}:preferences   # same hash tag = same shard
# All three keys land on the same Redis shard → MGET works atomically
```

**Rules:**
- Colons as namespace separators (universal convention)
- Short but descriptive — avoid `u:1` for `user:1001`
- Include the entity ID in the key, never bury it in the value only
- For temporary keys: add a TTL-hint in the name (`lock:`, `temp:`)
- For Cluster: use hash tags `{...}` to ensure related keys are on the same shard

---

## Part 4 — Coordination Patterns (full Lua implementations)

### Pattern 1 — Distributed Lock

```lua
-- acquire_lock.lua
-- KEYS[1] = lock key, ARGV[1] = holder ID, ARGV[2] = TTL seconds
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then
    return 1   -- acquired
end
return 0   -- already held

-- release_lock.lua
-- Only release if we still own it
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1   -- released
end
return 0   -- someone else owns it (we expired)
```

```python
import uuid

class RedisLock:
    def __init__(self, redis_client, resource: str, ttl: int = 30):
        self._redis = redis_client
        self._key = f"lock:{resource}"
        self._holder_id = str(uuid.uuid4())
        self._ttl = ttl

    async def acquire(self, retry_times=3, retry_delay=0.1) -> bool:
        for _ in range(retry_times):
            acquired = await self._redis.eval(ACQUIRE_LUA, 1,
                self._key, self._holder_id, self._ttl)
            if acquired:
                return True
            await asyncio.sleep(retry_delay)
        return False

    async def release(self):
        await self._redis.eval(RELEASE_LUA, 1, self._key, self._holder_id)

    async def __aenter__(self):
        if not await self.acquire():
            raise RuntimeError(f"Could not acquire lock: {self._key}")
        return self

    async def __aexit__(self, *_):
        await self.release()

# Usage:
async with RedisLock(redis, "payment:order:9001"):
    await process_payment(order_id=9001)
```

---

### Pattern 2 — Sliding Window Rate Limiter

```lua
-- rate_limit.lua
-- KEYS[1] = rate_limit:{identifier}
-- ARGV[1] = limit, ARGV[2] = window_seconds, ARGV[3] = current_time (float)
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local cutoff = now - window

-- Remove all entries outside the window
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)

-- Count remaining entries
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
    -- Add this request (score=time, member=time+random for uniqueness)
    redis.call('ZADD', KEYS[1], now, now .. ':' .. math.random(1000000))
    redis.call('EXPIRE', KEYS[1], math.ceil(window))
    return limit - count - 1  -- remaining quota
end

-- Calculate retry-after: when oldest entry in window expires
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if oldest[2] then
    return -(tonumber(oldest[2]) + window - now)  -- negative = retry after N seconds
end
return -window
```

```python
async def check_rate_limit(user_id: str, limit: int, window: int) -> tuple[bool, int]:
    """Returns (allowed, retry_after_seconds)"""
    import time
    result = await redis.eval(
        RATE_LIMIT_LUA, 1,
        f"rate_limit:{user_id}",
        limit, window, time.time()
    )
    if result >= 0:
        return True, 0       # allowed, N remaining
    return False, abs(result)  # rejected, retry after N seconds
```

---

### Pattern 3 — Token Bucket (burst-tolerant rate limiter)

```lua
-- token_bucket.lua
-- KEYS[1] = "bucket:{device_id}"
-- ARGV[1]=capacity, ARGV[2]=refill_rate_per_sec, ARGV[3]=tokens_to_consume, ARGV[4]=now
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])
local consume      = tonumber(ARGV[3])
local now          = tonumber(ARGV[4])

local data        = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens      = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

-- Compute tokens added since last call
local elapsed    = math.max(0, now - last_refill)
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= consume then
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens - consume, 'last_refill', now)
    redis.call('EXPIRE', KEYS[1], 3600)   -- auto-cleanup idle buckets
    return 1   -- allowed
end

redis.call('HMSET', KEYS[1], 'tokens', new_tokens, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], 3600)
return 0   -- rejected
```

---

### Pattern 4 — Counting Semaphore with TTL self-healing

```lua
-- semaphore_acquire.lua
-- KEYS[1] = "sem:{device_id}"
-- ARGV[1] = max_slots, ARGV[2] = slot_ttl_seconds, ARGV[3] = worker_id
local count = redis.call('HLEN', KEYS[1])
if tonumber(count) < tonumber(ARGV[1]) then
    local timestamp = redis.call('TIME')
    redis.call('HSET', KEYS[1], ARGV[3], timestamp[1] .. '.' .. timestamp[2])
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1   -- slot granted
end
return 0   -- all slots in use

-- semaphore_release.lua
redis.call('HDEL', KEYS[1], ARGV[1])
return 1

-- semaphore_renew.lua  (heartbeat)
-- Returns 1 if worker still has a slot, 0 if expired
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0   -- slot was reclaimed (worker was considered dead)
```

---

### Pattern 5 — Circuit Breaker

```lua
-- breaker_record.lua
-- KEYS[1] = "breaker:{device_id}:state"
-- KEYS[2] = "breaker:{device_id}:errors"
-- KEYS[3] = "breaker:{device_id}:total"
-- KEYS[4] = "breaker:{device_id}:timeouts"
-- ARGV[1] = outcome ("success"|"failure"|"timeout")
-- ARGV[2] = error_rate_threshold (e.g. 0.2 = 20%)
-- ARGV[3] = consecutive_timeout_threshold (e.g. 3)
-- ARGV[4] = window_seconds (e.g. 60)
-- ARGV[5] = now

local state    = redis.call('GET', KEYS[1]) or 'CLOSED'
local outcome  = ARGV[1]
local now      = tonumber(ARGV[5])

if outcome == 'success' then
    redis.call('INCR', KEYS[3])
    redis.call('SET', KEYS[4], 0)   -- reset consecutive timeouts
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[4]))

    if state == 'HALF_OPEN' then
        redis.call('SET', KEYS[1], 'CLOSED')
        redis.call('SET', KEYS[2], 0)
    end

elseif outcome == 'failure' then
    redis.call('INCR', KEYS[2])
    redis.call('INCR', KEYS[3])
    redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[4]))

    local errors = tonumber(redis.call('GET', KEYS[2]) or 0)
    local total  = tonumber(redis.call('GET', KEYS[3]) or 1)
    local rate   = errors / total

    if rate >= tonumber(ARGV[2]) and state == 'CLOSED' then
        redis.call('SET', KEYS[1], 'OPEN')
        redis.call('EXPIRE', KEYS[1], 60)   -- re-check (half-open) after 60s
    end

elseif outcome == 'timeout' then
    local ct = tonumber(redis.call('INCR', KEYS[4]))
    redis.call('EXPIRE', KEYS[4], tonumber(ARGV[4]))
    if ct >= tonumber(ARGV[3]) and state == 'CLOSED' then
        redis.call('SET', KEYS[1], 'OPEN')
        redis.call('EXPIRE', KEYS[1], 60)
    end
end

return redis.call('GET', KEYS[1]) or 'CLOSED'
```

---

### Pattern 6 — Job Scheduler (delayed execution)

Use Sorted Set with score = execute_at unix timestamp:

```python
async def schedule_job(job_id: str, payload: dict, run_at: float):
    # Save payload in DB (not in Redis — never put payload in Redis)
    await db.insert_job(job_id, payload, scheduled_at=run_at)
    # Only the job_id goes into Redis
    await redis.zadd("jobs:scheduled", {job_id: run_at})

async def poll_due_jobs():
    """Called every second by a beat scheduler."""
    now = time.time()
    # Get all jobs due in the next second
    due = await redis.zrangebyscore("jobs:scheduled", "-inf", now)
    for job_id in due:
        # Atomic claim: remove from scheduled, add to processing
        claimed = await redis.zrem("jobs:scheduled", job_id)
        if claimed:  # won the race (multiple pollers possible)
            await celery_app.send_task("process_job", args=[job_id])
```

---

### Pattern 7 — Fan-Out (one event → many recipients)

```python
# When a user posts a message, fan out to all followers
async def fan_out_post(author_id: str, post_id: str):
    followers = await db.get_followers(author_id)  # could be millions

    # Use pipeline for bulk writes
    pipe = redis.pipeline()
    for follower_id in followers:
        pipe.lpush(f"feed:{follower_id}", post_id)
        pipe.ltrim(f"feed:{follower_id}", 0, 999)  # keep last 1000
    await pipe.execute()

# Twitter optimisation: for celebrities with >1M followers,
# skip pre-computed fan-out; instead merge at read time:
async def get_feed(user_id: str):
    # Pre-computed feed for regular users
    feed = await redis.lrange(f"feed:{user_id}", 0, 49)

    # Merge in celebrity posts at read time (they didn't fan-out)
    celebrities_followed = await get_celebrities_followed(user_id)
    for celebrity_id in celebrities_followed:
        celebrity_posts = await redis.lrange(f"posts:{celebrity_id}", 0, 9)
        feed = merge_sorted(feed, celebrity_posts)[:50]

    return feed
```

---

## Part 5 — High Availability Architecture

### 5.1 Redis Sentinel — detailed

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Sentinel 1  │    │  Sentinel 2  │    │  Sentinel 3  │
│  (DC-A)      │    │  (DC-A)      │    │  (DC-B)      │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │ monitor
               ┌───────────┴───────────┐
               │                       │
        ┌──────▼──────┐        ┌───────▼─────┐
        │  Primary    │───────▶│  Replica    │
        │  (DC-A)     │  async │  (DC-B)     │
        └─────────────┘  repl  └─────────────┘
```

**Failover process (automatic):**
1. Sentinel 1 sends PING to primary → no response
2. Sentinel 1 marks primary as subjectively down (SDOWN)
3. Sentinel 1 asks Sentinel 2 and 3: "do you also see primary as down?"
4. Quorum (2 of 3) agrees → primary marked objectively down (ODOWN)
5. Sentinels elect a leader among themselves (Raft-like)
6. Leader Sentinel sends `SLAVEOF NO ONE` to best replica
7. Replica becomes new primary
8. Other replicas are re-pointed to new primary
9. Sentinels notify clients via pub/sub on sentinel channels
10. Total time: typically 15–30 seconds

**Python client with Sentinel:**
```python
from redis.sentinel import Sentinel

sentinel = Sentinel(
    [("sentinel-1", 26379), ("sentinel-2", 26379), ("sentinel-3", 26379)],
    socket_timeout=0.5
)
# Always gets current primary — auto-updates after failover
primary = sentinel.master_for("mymaster", socket_timeout=0.5)
replica = sentinel.slave_for("mymaster", socket_timeout=0.5)

# Reads from replica (lower load on primary)
value = replica.get("my:key")
# Writes to primary
primary.set("my:key", "value")
```

**Sentinel tuning parameters:**
```
sentinel down-after-milliseconds mymaster 5000    # 5s without response = SDOWN
sentinel failover-timeout mymaster 60000          # max 60s for failover
sentinel parallel-syncs mymaster 1               # 1 replica syncs at a time during failover
```

---

### 5.2 Redis Cluster — detailed

```
16,384 hash slots distributed across N shards:

Shard 1: slots 0–5460      → hash("user:1001") = 2841 → goes here
Shard 2: slots 5461–10922  → hash("order:9001") = 7723 → goes here
Shard 3: slots 10923–16383 → hash("product:42") = 14001 → goes here

Each shard = 1 primary + 1+ replicas
```

**Hash tag for co-location:**
```
# Without hash tags — these might land on different shards:
user:1001:profile     → slot X
user:1001:sessions    → slot Y
MGET user:1001:profile user:1001:sessions  → CROSSSLOT error!

# With hash tags — both use {user:1001} for slot calculation:
{user:1001}:profile   → slot Z (same for both)
{user:1001}:sessions  → slot Z (same shard)
MGET {user:1001}:profile {user:1001}:sessions  → works!
```

**The Cluster limitation for Lua scripts:**
```lua
-- This FAILS in Cluster if keys are on different shards:
local a = redis.call('GET', KEYS[1])
local b = redis.call('GET', KEYS[2])
-- KEYS[1] and KEYS[2] must be on the same slot
-- Solution: use hash tags so all KEYS[n] resolve to the same slot
```

**When to move from Sentinel to Cluster:**
- Dataset exceeds ~50GB on a single node (Redis is in-memory)
- Write throughput saturates a single primary's CPU (~100K ops/sec on modern hardware)
- You need geographic distribution of writes (multiple write primaries)

---

### 5.3 Replication lag and consistency

Redis replication is **asynchronous by default**. A write to primary is acknowledged
before the replica receives it.

```python
# Force synchronous replication (wait for at least 1 replica to confirm)
redis.set("critical:data", value)
await redis.execute_command("WAIT", 1, 1000)  # 1 replica, 1000ms timeout
# If WAIT returns 0 = replica did not confirm within 1 second
```

**When replication lag matters:** During a Sentinel failover, if the primary dies before
a write is replicated, that write is lost. For critical data (payments, bookings), either:
1. Use `WAIT` for synchronous replication before acknowledging the user
2. Use MSSQL/PostgreSQL as the source of truth; Redis only for fast dispatch

---

## Part 6 — Memory Management (production survival guide)

### 6.1 Internal encodings (why small objects are cheap)

Redis automatically switches between compact and full encodings based on thresholds:

| Structure | Compact encoding | Threshold | Full encoding |
|---|---|---|---|
| String | `int` or `embstr` | strings ≤ 44 bytes | `raw` |
| Hash | `listpack` | ≤ 128 fields, values ≤ 64B | `hashtable` |
| List | `listpack` | ≤ 128 elements, values ≤ 64B | `quicklist` |
| Set | `intset` | all integers, ≤ 512 members | `hashtable` |
| Set | `listpack` | ≤ 128 members, values ≤ 64B | `hashtable` |
| Sorted Set | `listpack` | ≤ 128 members, values ≤ 64B | `skiplist + hashtable` |

**Architect implication:** If you store 100 user IDs in a Set and they are all integers,
Redis uses `intset` — extremely compact (~8 bytes each). If you add one non-integer
("user:1001" instead of "1001"), Redis converts the entire Set to `hashtable` — ~10× more
memory. Design your keys and values with this in mind.

**Check encoding:**
```
OBJECT ENCODING my:key    → "intset" | "listpack" | "hashtable" | "skiplist" | etc.
```

---

### 6.2 Memory analysis commands

```bash
# Overall memory picture
redis-cli INFO memory

# Find the biggest keys (samples 1% of keyspace)
redis-cli --bigkeys

# Find hot keys (requires maxmemory-policy allkeys-lfu)
redis-cli --hotkeys

# Exact memory usage of one key
redis-cli MEMORY USAGE my:key              → bytes
redis-cli MEMORY USAGE my:key SAMPLES 5   → estimate for nested structures

# Memory breakdown by key pattern
redis-cli MEMORY DOCTOR   → advice on memory issues

# Sample keyspace for patterns
redis-cli --scan --pattern "user:*" | head -100

# Debug specific key internals
redis-cli OBJECT ENCODING my:key
redis-cli OBJECT REFCOUNT my:key
redis-cli OBJECT IDLETIME my:key   → seconds since last access (for LRU debugging)
```

**What to look for in `INFO memory`:**
```
used_memory:         105GB      → data in memory
used_memory_rss:     140GB      → OS-level RSS (includes fragmentation)
mem_fragmentation_ratio: 1.33   → RSS/used. >1.5 = fragmentation waste
maxmemory:           150GB
maxmemory_policy:    noeviction
mem_allocator:       libc        → jemalloc is better for fragmentation
```

**Fragmentation > 1.5:** Restart Redis (it will defrag on reload) or use
`MEMORY PURGE` (Redis 4.0+, forces jemalloc to return memory to OS).

---

### 6.3 Key expiry at scale

Redis expires keys two ways:
1. **Lazy expiry:** When a key is accessed, Redis checks if it has expired. If so, delete it.
2. **Active expiry:** 10 times per second, Redis samples 20 random volatile keys; if >25%
   are expired, repeat (up to 25% of cycle time).

**Problem:** If you set 10 million keys to expire at the same second (e.g., all cache
keys set during a batch job with `EX 3600`), at expiry time Redis will try to evict 10M
keys simultaneously → CPU spike → latency spike for all other operations.

**Solution: TTL jitter**
```python
import random

BASE_TTL = 3600
jitter = random.randint(-300, 300)   # ±5 minutes
redis.setex(key, BASE_TTL + jitter, value)
```

This spreads expiry across a 10-minute window instead of a single second.

---

### 6.4 The dangerous commands and safe alternatives

| Dangerous | Why | Safe alternative |
|---|---|---|
| `KEYS pattern` | O(N) over all keys, blocks server | `SCAN 0 MATCH pattern COUNT 100` |
| `SMEMBERS big_set` | Returns entire set in one response | `SSCAN key 0 COUNT 100` |
| `HGETALL big_hash` | Returns all fields | `HSCAN` or `HMGET` specific fields |
| `LRANGE list 0 -1` | Entire list | `LRANGE list 0 99` with pagination |
| `ZRANGE big_zset 0 -1` | All members | `ZRANGE list 0 99 LIMIT 0 100` |
| `FLUSHDB` | Deletes everything, blocks | `FLUSHDB ASYNC` |
| `DEBUG SLEEP 5` | Blocks server for 5 seconds | Never use in production |
| `CONFIG REWRITE` | Can corrupt config on crash | Backup first |

**SCAN usage:**
```python
# Correct: non-blocking paginated scan
cursor = 0
keys = []
while True:
    cursor, batch = redis.scan(cursor, match="session:*", count=100)
    keys.extend(batch)
    if cursor == 0:
        break  # full iteration complete
```

---

## Part 7 — Security

### 7.1 Authentication

```
# redis.conf
requirepass your-strong-password-here

# Python
redis.Redis(host="...", password="your-strong-password")
```

### 7.2 ACL (Access Control Lists) — Redis 6.0+

Different clients get different permissions. Never give your application the password
that can `FLUSHALL` or `CONFIG SET`.

```
# redis.conf ACL rules
ACL SETUSER app-readonly on >app-readonly-password ~* &* +@read
ACL SETUSER app-writer on >app-writer-password ~user:* ~session:* +@write +@read -DEBUG
ACL SETUSER admin on >admin-password allkeys allchannels allcommands

# Check current user's permissions
ACL WHOAMI
ACL LIST
```

**Real scenario:** Your Celery workers only need to read task IDs and write status updates.
Give them a user that can only `GET`, `SET`, `HSET`, `EXPIRE` on their key patterns.
If a worker is compromised, the attacker cannot `KEYS *`, `FLUSHDB`, or read other services' data.

### 7.3 Disable dangerous commands

```
# redis.conf — rename to disable
rename-command FLUSHALL ""
rename-command FLUSHDB ""
rename-command CONFIG ""
rename-command DEBUG ""
rename-command KEYS ""
```

### 7.4 TLS

```
# redis.conf
tls-port 6380
tls-cert-file /path/to/cert.pem
tls-key-file /path/to/key.pem
tls-ca-cert-file /path/to/ca.pem
tls-auth-clients yes   # require client certificates
```

### 7.5 Network isolation

Never expose Redis to the public internet. Use:
- `bind 127.0.0.1 10.0.0.5` — only internal interfaces
- Network security groups / firewall rules: only app pods can reach Redis port
- OpenShift NetworkPolicy: `ingress from app-pod-label to redis-pod`

---

## Part 8 — Industry Use Cases (with real numbers)

### 8.1 Twitter — 105TB RAM, 39M QPS

**Stack:** 10,000+ Redis instances across multiple DCs.

**Problem solved:** At peak, a single viral tweet by a user with 100M followers requires
100M List pushes (fan-out). If done synchronously, posting a tweet takes minutes.

**Architecture:**
- Timeline cache: each user has a List of tweet IDs, capped at 800 entries
- Fan-out: when you tweet, a background job pushes your tweet ID to followers' Lists
- Celebrities (>1M followers): skip pre-computed fan-out; merged at read time
- Active users: List is kept warm in Redis; inactive users (30 days): evicted, rebuilt on next login

**Scale facts:**
- 30 billion Redis updates per day
- 39 million queries per second
- Average 105TB of data in RAM at any time
- 10,000+ instances manage this data

**Lesson:** Cache only what is accessed. Evict cold data aggressively. Use background
fan-out to decouple write latency from read latency.

---

### 8.2 GitHub — API rate limiting

**Problem:** 5,000 requests per hour per token across hundreds of API servers. Old solution
(Memcached) competed with application cache for memory, causing reliability issues.

**Solution:** Dedicated Redis cluster just for rate limiting.
- Completely separate from application cache — no memory competition
- Client-side sharding: each API server deterministically picks the Redis shard for each user
- One primary per shard, 2 replicas for read redundancy
- Rate limit key: `rate_limit:{token_id}` → Sorted Set with timestamps as scores

**Migration result:** Eliminated rate limit reliability issues entirely. Reduced customer
support tickets about rate limiting by a significant margin.

**Lesson:** Separate Redis instances by concern. A rate-limiting Redis instance should
never share memory with a caching Redis instance.

---

### 8.3 Uber — real-time driver location

**Problem:** 5M+ active drivers worldwide sending GPS updates every 4 seconds.
Rider querying "show me nearby drivers" must return in under 100ms.

**Architecture:**
- City map divided into H3 hexagonal cells (~300m diameter at resolution 8)
- Each cell = one Sorted Set: `drivers:{city}:{cell_id}` — member=driver_id, score=timestamp
- Driver update: `ZADD drivers:{city}:{cell} {timestamp} {driver_id}` (4 updates/second)
- Rider query: look up cell rider is in + 6 neighboring cells → ZRANGE on 7 sets → deduplicate
- Ghost driver removal: `ZRANGEBYSCORE drivers:{city}:{cell} 0 {now-30}` → delete stale

**Why not native Redis GEO?** Redis GEO uses fixed geohash precision. Uber needed variable
precision matching H3 cell sizes for their surge pricing algorithms. Custom Sorted Set
gave full control.

**Scale:** Millions of GPS updates per second. Sub-50ms response for all proximity queries.

---

### 8.4 Discord — presence and messaging

**Problem:** 19M+ concurrent users. Must show online/idle/DND status in real time.
Large servers (Discord calls them guilds) can have 100K+ members.

**Architecture:**
- Presence: Hash per user — `user:{id}:presence` → {status, since, rich_presence}
- WebSocket heartbeat every 45s → HSET to update presence hash
- Guild events: PUBLISH to `guild:{id}:events` → all gateway pods for that guild consume
- Large guilds: split across multiple gateway pods, all subscribed to same Pub/Sub channel

**Scale:** For a 100K-member guild, a single message triggers 100K+ delivery operations.
Pub/Sub lets Redis fan out to all gateway pods in one PUBLISH; each pod handles its
connected WebSocket clients.

**Lesson:** Pub/Sub is extremely efficient for fan-out where fire-and-forget is acceptable.
A missed heartbeat update is self-correcting (next heartbeat 45s later). Use Streams
instead if you need guaranteed delivery.

---

### 8.5 Pinterest — 70M-user follower graph

**Problem:** 70M users, billions of follower relationships. Feed generation requires
checking follower relationships thousands of times per second.

**Architecture:**
- Each user's follower Set: `user:{id}:followers` — all follower user IDs
- Sharded across Redis instances by `user_id % num_shards`
- `SISMEMBER user:1001:followers 1002` → O(1) "does 1001 follow 1002?"
- `SCARD user:1001:followers` → follower count
- AOF persistence: entire follower graph is durable

**Why not a database?** `SELECT COUNT(*) FROM follows WHERE followee_id = ?` with a
database JOIN adds 5–10ms per check. At 10,000 feed-generation calls per second, that is
50–100 seconds of DB time per second. Redis reduces this to microseconds.

**Memory:** Average 200 followers × 8 bytes (int-encoded user ID) × 70M users ≈ 112GB.
Fits on a few Redis nodes; not affordable in a database's working memory.

---

### 8.6 Netflix — personalization and A/B testing

**Problem:** 200M+ subscribers, personalised recommendations must serve in under 20ms.
A/B test cohort assignment must be consistent and fast.

**Architecture:**
- User profiles and recommendation vectors: cached in Redis as JSON (via RedisJSON module)
- A/B test assignment: Bitmap per experiment → `SETBIT experiment:homepage_v2 {user_id} 1`
  → O(1) check for which variant a user is in
- Playback session state: Hash per session → stored in Redis for fast access during playback
- Image serving: each image variant URL cached per device type

**A/B testing at scale:**
```
10M users, 50 concurrent A/B tests
= 50 bitmaps × 10M bits each
= 50 × 1.25MB = 62.5MB total
```
Checking if user 5,432,101 is in experiment "homepage_v2":
`GETBIT experiment:homepage_v2 5432101` → 0 or 1, in microseconds.

---

### 8.7 Stripe — idempotency and rate limiting

**Problem:** Network failures cause clients to retry payments. Charging a customer twice
for a retry is a serious bug. Also, fraudulent clients make thousands of API calls per second.

**Idempotency architecture:**
- Client generates a UUID `Idempotency-Key` header for each payment request
- Server: `SET idempotency:{key} "processing" NX EX 86400` → NX means "only if not exists"
- If NX fails: another request with this key is in flight → return 409
- After completion: update key to store the response body
- On retry (client got no response): key exists with response → return it, no second charge

**Rate limiting:**
- Token bucket per API key, per endpoint, per second
- More permissive buckets for trusted customers, stricter for new accounts
- Rate limit state in Redis Cluster — all API servers see the same counters

**Lesson:** `SET NX` (set-if-not-exists) is the foundation of distributed idempotency.
It is safe, atomic, and requires zero coordination beyond a Redis instance.

---

### 8.8 Shopify — flash sale traffic absorption

**Problem:** A brand announces a flash sale. In the first second, 500,000 requests hit
the checkout API simultaneously. The database cannot handle this.

**Architecture:**
- Queue-based purchase flow: user clicks Buy → request is queued in Redis Stream
- Worker pool drains the stream at a controlled rate (database-safe)
- User sees "You're in line — estimated wait: 2 minutes"
- No requests hit the database until a worker processes them from the stream

```
500K requests/second → Redis Stream → Workers drain at 5K/second → Database
   (Redis handles this)     (backlog)    (controlled rate)          (healthy)
```

**Inventory reservation:**
```lua
-- Atomically reserve one item
local stock = tonumber(redis.call('GET', KEYS[1]) or 0)
if stock > 0 then
    redis.call('DECR', KEYS[1])
    return 1   -- reserved
end
return 0   -- sold out
```

**Lesson:** Redis absorbs traffic spikes that would kill a database. The database is
the bottleneck; Redis is the buffer that smooths the load curve.

---

## Part 9 — The Architect's Decision Checklist

When someone says "we should use Redis for X", ask these questions:

**1. Can we afford to lose this data?**
- YES → Redis alone is fine (cache, rate limits, ephemeral queues)
- NO → Database is source of truth; Redis is a fast read layer

**2. Does this data fit in RAM?**
- YES → Redis is appropriate
- NO → Hot subset in Redis, cold data in database; or use RDB/AOF for full durability

**3. Do multiple pods need to share this state?**
- YES → Redis (shared state)
- NO → In-process cache is simpler and faster

**4. Is the operation a read-then-write?**
- YES → Must use Lua script (atomic) or WATCH (optimistic lock)
- NO → Simple GET/SET/INCR is fine

**5. Sentinel or Cluster?**
- Single-node dataset fits in RAM: Sentinel
- Dataset > single node capacity: Cluster (with hash tag planning)

**6. What eviction policy?**
- Celery queues or coordination state: `noeviction`
- Pure cache (all data expendable): `allkeys-lru` or `allkeys-lfu`
- Mixed (cache + coordination): `volatile-lru` (set TTLs on cache, no TTL on coordination)

**7. What persistence?**
- Temporary cache (loss OK on restart): `save ""` (no persistence)
- Queue + coordination (cannot lose): AOF `everysec` + RDB
- Critical transactions: Database is source of truth; Redis is not persistence

---

## Part 10 — Interview Questions by Level

### Junior level (can you use Redis?)

1. What is the difference between a Redis String and a Hash?
2. How do you set a key that expires in 5 minutes?
3. What does `INCR` do and why is it safe without a lock?
4. What is the difference between `LPUSH` and `RPUSH`? When would you use each?
5. What does `SISMEMBER` do and what is its time complexity?
6. How would you implement a counter that resets every hour?
7. What is TTL and why should cache keys always have one?

### Senior level (can you design with Redis?)

1. Design a rate limiter that allows 100 requests per minute per user. How do you handle
   the case where two servers check the limit simultaneously?
2. What is a cache stampede and how do you prevent it?
3. When would you use a Sorted Set instead of a List?
4. Explain the difference between Pub/Sub and Streams. When would you use each?
5. What is the difference between `SET key value NX` and `SETNX key value`?
6. How does MULTI/EXEC differ from a Lua script? Which is better for conditional operations?
7. A developer proposes storing the full job payload in a Celery queue. What's the risk?
8. What is `maxmemory-policy noeviction` and when must you use it?

### Architect level (can you build production systems with Redis?)

1. Your Redis has 80% memory used. `maxmemory-policy` is `allkeys-lru`. What data might
   be silently disappearing and how would you detect it?
2. You have 4 pods each with their own in-process circuit breaker. Why is this wrong and
   how do you fix it?
3. Describe how to implement a distributed lock that is safe against the holder crashing.
   What is the race condition in a naive implementation?
4. When does Redis Cluster break multi-key operations and how do you design around it?
5. A Lua script works in your single-Redis dev environment but throws CROSSSLOT errors in
   Cluster production. Why and how do you fix it?
6. Your application uses `KEYS pattern` in one background job. What happens when the
   keyspace reaches 10 million keys?
7. Design a real-time leaderboard for 50 million players. What data structure, what key
   design, what eviction policy, and how do you serve the top 100 at sub-millisecond latency?
8. How would you implement a "sliding window" rate limiter that is accurate, atomic, and
   works across 20 pods?
9. Your Redis primary dies. Sentinel promotes a replica. What writes were lost and how do
   you detect them?
10. A hot key (celebrity user profile) receives 500,000 requests per second. Redis is
    single-threaded. The key is bottlenecked on one CPU. What are your options?

**Answers to the 10 architect questions:**

1. Any key used by Celery tasks (queued jobs). Detect with `INFO stats` → `evicted_keys > 0`.
   Fix: use `noeviction` for queues; separate instances for cache and queues.

2. One pod opens its breaker; other 3 pods don't know. All 3 keep hammering the failing
   device. Fix: store circuit breaker state in Redis — one key per device, visible to all pods.

3. `SET key holder_id NX EX ttl`. Race condition: lock expires, another pod acquires it,
   original pod (recovered from hang) deletes it — deletes someone else's lock. Fix: Lua
   release script that checks the holder_id before deleting.

4. Cluster breaks `MGET`/`MSET`/`EVAL` on keys that hash to different slots.
   Fix: hash tags `{user:1001}:profile` and `{user:1001}:sessions` — both hash on
   `user:1001` → same slot.

5. KEYS[1] and KEYS[2] in the script hash to different slots — Redis Cluster refuses.
   Fix: use hash tags to ensure all keys in the script are on the same slot.

6. `KEYS *` iterates all 10M keys on the single-threaded Redis. Takes seconds. Blocks
   every other client during that time. Fix: `SCAN 0 MATCH pattern COUNT 100` (paginated).

7. Sorted Set `ZADD leaderboard {score} {player_id}`. One key per game. `ZREVRANK` for
   rank lookup. `ZREVRANGE 0 99 WITHSCORES` for top 100. Eviction: `volatile-lru` with
   long TTL on the leaderboard key. For 50M players, sorted set is ~2–3GB — fits on one node.

8. Sorted Set sliding window in a Lua script: `ZREMRANGEBYSCORE` → `ZCARD` → `ZADD` if
   allowed → all atomic. One key per user per endpoint. TTL = window size for auto-cleanup.

9. Replication is async. Writes acknowledged by primary but not yet replicated are lost.
   Detect: `redis-cli DEBUG JMAP` on replica vs primary. Prevent: `WAIT 1 1000` before
   acknowledging critical writes. For non-critical data (cache, rate limits): accept the loss.

10. Options in order of complexity: (a) L1 in-process cache — serve from process memory
    for 60s, reduce Redis load 100×. (b) Local cache per pod with Pub/Sub invalidation.
    (c) Read replicas with client-side load balancing for read queries. (d) Key sharding —
    `celebrity:1001:profile:shard:{N}` across N keys, aggregate at read time.

---

## Part 11 — 20-Day Study Plan

**Start date:** 2026-08-18

### Week 1 — Foundations

**Day 1 (Mon 18 Aug) — Setup and Strings**
- Install Redis with Docker: `docker run -d -p 6379:6379 --name redis redis:7`
- Open `redis-cli` and run every command in Part 1.1
- Build: a page view counter per URL that resets daily (key includes the date)
- Build: a session store with 1-hour TTL
- Question to answer before bed: why is `INCR` safe without a lock?

**Day 2 (Tue 19 Aug) — Hash and List**
- Run every command in Parts 1.2 and 1.3
- Build: store a user profile in a Hash. Add a `login_count` field. Increment it atomically.
- Build: a FIFO task queue using List. Push 10 jobs. Pop them one by one.
- Build: the reliable queue pattern (LMOVE source → processing → LREM on completion)
- Question: what happens to a List job if the worker crashes between LPOP and completion?

**Day 3 (Wed 20 Aug) — Set and Sorted Set**
- Run every command in Parts 1.4 and 1.5
- Build: tag system for articles using Sets. Find articles with 2 specific tags (SINTER).
- Build: a leaderboard for a game. Add 10 players. Update scores. Query top 3.
- Build: the sliding window rate limiter from Part 4 Pattern 2 — test with 2 concurrent clients
- Question: why can't you use a List for "has user X been seen today?"

**Day 4 (Thu 21 Aug) — Stream and HyperLogLog**
- Run every command in Parts 1.6 and 1.7
- Build: an event stream. Produce 10 events. Create a consumer group. Consume with 2 consumers.
- Kill one consumer after consuming but before ACKing. Use XPENDING + XCLAIM to recover.
- Build: unique visitor counter using HyperLogLog vs a Set. Compare memory usage at 10K users.
- Question: what is the difference between Stream consumer groups and Pub/Sub?

**Day 5 (Fri 22 Aug) — Caching patterns**
- Implement all 3 caching patterns from Part 2 (cache-aside, write-through, write-behind)
- Simulate a cache stampede: expire a hot key, hit it from 10 goroutines simultaneously, watch the DB
- Fix it with the mutex lock pattern (Part 2.4 Solution 2)
- Read: `redis.conf` in this project, understand every line
- Question: when would you choose write-behind over write-through?

**Weekend (23–24 Aug):**
- Read the GitHub engineering blog post about their sharded rate limiter
- Read the Twitter Redis architecture post (highscalability.com)
- Try: `redis-cli --bigkeys` on a populated Redis instance
- Write down: "for my current project, which caching pattern would I use and why?"

---

### Week 2 — Intermediate

**Day 6 (Mon 25 Aug) — Lua scripting**
- Run the token bucket Lua script from Part 4 Pattern 3 manually via `redis-cli EVAL`
- Port it to Python using `redis.eval()`
- Break it: run 5 concurrent Python threads all calling consume() without Lua — observe double-spending
- Fix it with the Lua version — observe correct behaviour
- Build: a distributed lock using the acquire/release Lua scripts from Part 4 Pattern 1
- Question: what is the difference between Lua atomicity and MULTI/EXEC?

**Day 7 (Tue 26 Aug) — Memory management**
- `docker exec -it redis redis-cli INFO memory` — understand every field
- Set 100,000 keys with no TTL. Run `INFO memory`. Add a TTL to all of them with SCAN + EXPIRE.
- Run `OBJECT ENCODING` on a Hash with 5 fields vs 200 fields — observe encoding change
- Calculate memory for: 1M users, each with a Hash of 10 fields (5 bytes each)
- Set `maxmemory 10mb` with `allkeys-lru`, fill Redis past 10mb, observe evictions in `INFO stats`
- Switch to `noeviction`, try to write past maxmemory, catch the OOM error in Python

**Day 8 (Wed 27 Aug) — WATCH, pipelines, keyspace notifications**
- Implement the transfer_tokens example from Part 3.2 using WATCH
- Implement the same with a Lua script — compare code complexity
- Benchmark: 1000 individual GETs vs pipelining 1000 GETs in one batch — measure time difference
- Configure keyspace notifications (`notify-keyspace-events Ex`), subscribe, set a key with TTL,
  watch the expiry event arrive in your subscriber

**Day 9 (Thu 28 Aug) — Sentinel setup**
- Start 1 primary + 2 replicas + 3 Sentinels using Docker Compose
- Verify replication: write to primary, read from replica
- Kill the primary container: `docker stop redis-primary`
- Watch Sentinel logs as it detects, votes, and promotes a replica (~15–30 seconds)
- Connect a Python client through Sentinel, verify it auto-discovers the new primary
- Question: what writes were lost during the failover window?

**Day 10 (Fri 29 Aug) — Redis Cluster**
- Start a 6-node cluster (3 primary + 3 replica) using docker-compose or redis-cli --cluster create
- `CLUSTER KEYSLOT user:1001` — see which slot this key maps to
- `CLUSTER KEYSLOT {user:1001}:profile` vs `CLUSTER KEYSLOT {user:1001}:sessions` — same slot
- Try `MGET user:1001:a user:1002:b` — observe CROSSSLOT error
- Try `MGET {user:1001}:a {user:1001}:b` — observe success (same hash tag = same slot)
- Write a Lua script that uses 2 keys — test it fails in Cluster without hash tags, succeeds with them

**Weekend (30–31 Aug):**
- Try Redis Modules: start Redis Stack (`docker run -p 6379:6379 redis/redis-stack`)
- Play with `JSON.SET`, `JSON.GET`, `JSON.NUMINCRBY`
- Play with `FT.CREATE` index and `FT.SEARCH` — build a searchable product catalog
- Play with `BF.ADD` and `BF.EXISTS` — check false positive rate with 10K items

---

### Week 3 — Advanced patterns

**Day 11 (Mon 1 Sep) — Build a complete rate limiter**
- Implement the sliding window rate limiter (Part 4 Pattern 2) with full Python class
- Add retry-after calculation (how long until quota resets)
- Test: 10 concurrent threads, limit=5/minute — verify exactly 5 succeed and 5 are rejected
- Test: wait 30 seconds, verify 5 more succeed (window rolled forward)
- Add to a FastAPI endpoint as a dependency

**Day 12 (Tue 2 Sep) — Build a counting semaphore**
- Implement DeviceSemaphore from Part 4 Pattern 4 with all 3 Lua scripts
- Test: 5 concurrent workers trying to acquire, max_slots=3 — verify exactly 3 succeed
- Kill a worker without releasing — verify its slot expires via TTL after `slot_ttl` seconds
- Implement the heartbeat renewal — verify a long-running worker keeps its slot
- Wire it to a mock "workflow" function that takes 10 seconds

**Day 13 (Wed 3 Sep) — Build a circuit breaker**
- Implement a 3-state circuit breaker from Part 4 Pattern 5
- Wire it to a mock HTTP client that fails 30% of the time and has 200ms latency
- Watch the breaker open when error rate exceeds threshold
- Watch the breaker go half-open, allow one probe, and close on success
- Test: 2 "pods" sharing the same Redis breaker key — verify one pod's failures open the
  breaker for both pods

**Day 14 (Thu 4 Sep) — Build a geospatial driver system**
- Add 1000 mock driver positions with GEOADD
- Query "drivers within 2km of a rider" with GEOSEARCH
- Implement zombie detection: ZRANGEBYSCORE to find drivers who haven't updated in 30s → remove
- Implement surge pricing: count drivers in each H3 cell, compute supply/demand ratio
- Bonus: implement the H3-style cell sharding (create a Sorted Set per region)

**Day 15 (Fri 5 Sep) — Build a cache stampede prevention system**
- Build a high-traffic endpoint that caches an expensive DB query
- Simulate 100 concurrent requests with the cache expired
- Without protection: observe the DB being hit 100 times simultaneously
- Add mutex lock protection: observe only 1 DB hit, others wait and use cached result
- Add stale-while-revalidate: observe 0 extra latency for users, refresh happens in background

**Weekend (6–7 Sep):**
- Architecture exercise: design a Redis architecture for a food delivery app:
  - Driver location tracking
  - Order status (real-time updates to customers)
  - Restaurant menu caching
  - Rate limiting on the ordering API
  - For each: data structure, key design, eviction policy, TTL strategy
- Write it up as a 1-page design doc

---

### Week 4 — Architect mindset

**Day 16 (Mon 8 Sep) — Failure mode analysis**
- Run every failure scenario from Part 9's checklist against your running Redis
- What happens when Redis `noeviction` is full and Celery tries to `.delay()`?
  Catch the exception in Python. Return 503 correctly.
- What happens to WATCH transactions during a Sentinel failover?
  Simulate a failover mid-transaction. Observe WatchError. Handle correctly.
- What happens to in-flight Lua scripts during a restart?
  Simulate: kill Redis mid-script. What state is left?

**Day 17 (Tue 9 Sep) — Anti-pattern hunting**
- Take any production codebase you can access (yours or open source)
- Find and document: missing TTLs, use of KEYS *, payload-in-queue, missing maxmemory
- For each: write the correct version
- Run `redis-cli --latency-history -i 1` while running the dangerous commands — see latency spikes

**Day 18 (Wed 10 Sep) — Redis in this F5 project**
- Read `app/coordination/semaphore.py` + all 3 semaphore Lua scripts fully
- Read `app/coordination/ratelimit.py` + token_bucket.lua fully
- Read `app/coordination/breaker.py` + both breaker Lua scripts fully
- For each: trace through what happens when Redis is unavailable (follow the exception path)
- Question: what would happen if `maxmemory-policy` was `allkeys-lru` on this project's Redis?

**Day 19 (Thu 11 Sep) — Architecture design exercises**
- Design 3 systems, each requiring a different Redis pattern:
  1. Real-time multiplayer game leaderboard — 10M players, 1000 score updates/second
  2. Banking API — idempotency, rate limiting, session management
  3. IoT sensor monitoring — 100K sensors reporting every 5 seconds, alert on anomaly
- For each: pick data structures, design keys, pick eviction policy, decide Sentinel vs Cluster

**Day 20 (Fri 12 Sep) — Review and consolidation**
- Attempt all 10 architect interview questions (Part 10) without looking at the answers
- Compare your answers to the provided answers
- Write your personal Redis cheat sheet: "when I need X, I reach for Y because Z"
- Identify your weakest area from the 10 questions — spend 30 minutes going deeper on it

---

### Continuing education (after Day 20)

| Topic | Resource |
|---|---|
| Deep data structure internals | *Redis in Action* — Josiah Carlson (O'Reilly) |
| Redis source code | github.com/redis/redis — read `t_hash.c`, `t_zset.c` for data structure implementations |
| Production patterns | antirez.com/news — Redis creator's blog |
| Real engineering posts | engineering.twitter.com, github.blog, discord.com/blog |
| Redis Streams deep dive | redis.io/docs/latest/develop/data-types/streams/ |
| Redis Modules | redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/ |
| Distributed systems theory | *Designing Data-Intensive Applications* — Martin Kleppmann (ch. 5–9) |
| Redis internals | *Redis Design and Implementation* (Chinese, some content in English) |

---

### The 10 signs you are thinking like a Redis architect

1. You immediately ask "what is the eviction policy?" when someone mentions Redis
2. You know that `INCR` is atomic without needing a lock, and you know WHY
3. You reach for Lua scripts when you need atomic read-then-write
4. You design key names with Cluster hash slots in mind, even when not using Cluster yet
5. You separate Redis instances by concern (cache, queues, coordination)
6. You set TTLs on every cache key AND add jitter to prevent mass-expiry spikes
7. You treat Redis as a fast coordination layer, not a persistent store
8. You know that a timeout from an external service is UNKNOWN, not a failure
9. You know that Sentinel failover loses in-flight writes and design around that
10. When someone says "Redis is down" you ask "what does the application do?" not "how do we fix Redis?"
