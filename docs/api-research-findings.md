# API Research Findings

Confirmed from official F5 CloudDocs and Infoblox WAPI documentation.
These are the authoritative shapes for the client layer (T-0.2, T-0.8).

---

## F5 iControl REST — GTM/DNS Objects

### Base path
All GTM resources live under `/mgmt/tm/gtm/`.

### WideIP (type A)
- Collection: `GET /mgmt/tm/gtm/wideip/a`
- Resource:   `GET|PUT|PATCH|DELETE|POST /mgmt/tm/gtm/wideip/a/~{partition}~{name}`
- Natural key: `name`, `partition`, `subPath`
- Partition separator in REST path: `~` (e.g. `~Common~my.fqdn.example.com`)

Key fields (all optional per spec — `name` is the natural key, sent in path):
| Field | Type | Notes |
|---|---|---|
| `name` | string | FQDN |
| `partition` | string | default `Common` (field name in payload: `partition`) |
| `pools` | array | pool references, each with `name`, `order`, `ratio` |
| `poolsCname` | array | CNAME pool references |
| `poolLbMode` | string | `round-robin`, `ratio`, etc. |
| `persistence` | string | `disabled` (default) or `enabled` |
| `persistCidrIpv4` | integer | default 32 |
| `persistCidrIpv6` | integer | default 128 |
| `ttlPersistence` | integer | default 3600 |
| `enabled` / `disabled` | boolean | |
| `failureRcode` | string | default `noerror` |
| `failureRcodeResponse` | string | default `disabled` |
| `failureRcodeTtl` | integer | default 0 |
| `lastResortPool` | string | |
| `minimalResponse` | string | default `enabled` |

**WideIP types**: `a`, `aaaa`, `cname`, `mx`, `naptr`, `srv` — each is a separate endpoint.

### GTM Pool (type A)
- Collection: `GET /mgmt/tm/gtm/pool/a`
- Resource:   `GET|PUT|PATCH|DELETE|POST /mgmt/tm/gtm/pool/a/~{partition}~{name}`
- Sub-collection for members: `/mgmt/tm/gtm/pool/a/~{partition}~{name}/members`

Key fields:
| Field | Type | Notes |
|---|---|---|
| `name` | string | pool name |
| `partition` | string | |
| `monitor` | string | monitor reference, e.g. `/Common/my-monitor` |
| `loadBalancingMode` | string | e.g. `round-robin`, `ratio`, `least-connections` |
| `alternateMode` | string | |
| `fallbackMode` | string | |
| `dynamicRatio` | string | |
| `ttl` | integer | |
| `maxAnswersReturned` | integer | |
| `verifyMemberAvailability` | string | `enabled`/`disabled` |
| `enabled` / `disabled` | boolean | |

**Pool types**: same as WideIP — `a`, `aaaa`, `cname`, `mx`, `naptr`, `srv`.

### GTM Pool Members
- Sub-collection on the pool resource: `.../members`
- PATCH the parent pool with an updated `members` array, or POST/DELETE to the members sub-collection
- Member fields include `name` (VS reference), `order`, `ratio`, `limitMaxBps`, `limitMaxConnections`

### GTM Monitor (type bigip)
- Collection: `GET /mgmt/tm/gtm/monitor/bigip`
- Resource:   `GET|PUT|PATCH|DELETE|POST /mgmt/tm/gtm/monitor/bigip/~{partition}~{name}`

Key fields: `name`, `destination`, `interval` (int), `timeout` (int), `defaultsFrom`, `ignoreDownResponse`

**Monitor types available**: `bigip`, `http`, `https`, `tcp`, `udp`, `gateway-icmp`, `dns`, `external`, `scripted`, and 20+ more.

### Transactions
**GTM objects do NOT appear in the iControl REST transaction documentation.** Transactions are confirmed NOT supported for GTM objects. Each object type must be created/modified in separate API calls. The four F5 steps (monitor → pool → members → WideIP) cannot be collapsed into one atomic transaction.

### Authentication
- **Login**: `POST /mgmt/shared/authn/login`
  ```json
  {"username": "...", "password": "...", "loginProviderName": "tmos"}
  ```
  Response contains `token.token` (the value to use) and `token.timeout`.
- **Default token lifetime**: 1200 seconds (20 minutes)
- **Maximum token lifetime**: 36000 seconds (10 hours)
- **Extend token**: `PATCH /mgmt/shared/authz/tokens/{token_value}` with `{"timeout": 36000}`
- **Auth header on subsequent calls**: `X-F5-Auth-Token: {token_value}`
- **Concurrent refresh guard**: Must be implemented in code (Redis lock) to prevent token stampede — the F5 API itself does not prevent multiple concurrent logins.

---

## Infoblox WAPI — CNAME Records

### Base URL
`https://{grid-master}/wapi/v{version}/`

Common versions: 2.10, 2.12, 2.13 — field names for `record:cname` are consistent across 2.x.

### CNAME Record
- Object type: `record:cname`
- Create: `POST /wapi/v{version}/record:cname`
- Read: `GET /wapi/v{version}/record:cname?name={fqdn}&view={view}`
- Update: `PUT /wapi/v{version}/{_ref}` where `_ref` is the opaque reference returned on create
- Delete: `DELETE /wapi/v{version}/{_ref}`

Fields:
| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | **Yes** | The alias FQDN |
| `canonical` | string | **Yes** | The target FQDN |
| `view` | string | No | DNS view; omit to use default view |
| `ttl` | unsigned int | No | TTL in seconds |
| `use_ttl` | bool | No | Must be `true` for `ttl` to take effect |
| `comment` | string | No | |
| `disable` | bool | No | Default false |
| `extattrs` | object | No | Extensible attributes |

Read-only fields returned in responses: `zone`, `reclaimable`, `creation_time`, `last_queried`, `dns_name`, `dns_canonical`, `_ref`

### Authentication
- **Session init**: Make any WAPI call with `Authorization: Basic {b64(user:pass)}` header.
  The response will include a `Set-Cookie: ibapauth=...` header.
- **Subsequent calls**: Include `Cookie: ibapauth={value}` — no re-authentication needed.
- **Session invalidation**: `POST /wapi/v{version}/logout`
- **Important**: All WRITE operations must target the **grid master** host. Read operations can go to any grid member.

### Idempotency behaviour
- Creating a CNAME that already exists returns an error (HTTP 400 with Infoblox error body).
- Safe idempotent pattern: GET first to check existence. If exists and `canonical` matches → no-op. If exists and `canonical` differs → PUT with `_ref` to update. If not exists → POST to create.
- Delete of a non-existent `_ref` returns HTTP 404. Treat as successful (already gone).

---

## Implications for T-0.5

Transactions are NOT supported for GTM objects. The four F5 create steps (monitor → pool → members → WideIP) remain four separate API calls. This means partial states are possible and the rollback mechanism is essential. T-4.6 (transaction path) is CANCELLED.

---

*Sources confirmed from:*
- https://clouddocs.f5.com/api/icontrol-rest/APIRef_tm_gtm_wideip_a.html
- https://clouddocs.f5.com/api/icontrol-rest/APIRef_tm_gtm_pool_a.html
- https://clouddocs.f5.com/api/icontrol-rest/APIRef_tm_gtm_monitor_bigip.html
- https://clouddocs.f5.com/api/icontrol-rest/APIRef_tm_gtm.html
- https://ipam.illinois.edu/wapidoc/objects/record.cname.html
- F5 DevCentral: iControl REST Authentication Token Management
