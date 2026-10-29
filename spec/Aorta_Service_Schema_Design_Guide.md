# Aorta Service Schema Design Guide

**Version:** 1.0  
**Date:** 2025-01-20  
**Status:** Draft

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Schema Registration & Discovery](#2-schema-registration--discovery)
3. [Schema Validation & Conflict Handling](#3-schema-validation--conflict-handling)
4. [Attachment Policy for RPC](#4-attachment-policy-for-rpc)
5. [Best Practices](#5-best-practices)
6. [Example Implementation](#6-example-implementation)
7. [FAQ](#7-faq)

---

## 1. Design Principles

### 1.1 Schema Naming Convention

Service RPC schemas **MUST** follow a consistent naming pattern to enable tooling automation:

#### Request/Response Pairing

```fbs
// Pattern 1: Full suffix (recommended)
table GetPlanRequest { ... }
table GetPlanResponse { ... }

// Pattern 2: Short suffix (acceptable)
table GetPlanReq { ... }
table GetPlanRes { ... }

// ❌ INVALID: Inconsistent or missing pairing
table GetPlan { ... }           // Missing Request/Response suffix
table GetPlanRequest { ... }
table GetPlanReply { ... }      // Inconsistent suffix
```

**Rules:**
- Request: `{ServiceName}Request` or `{ServiceName}Req`
- Response: `{ServiceName}Response` or `{ServiceName}Res`
- Use **either** full OR short suffix consistently within a file
- ServiceName **MUST** match exactly between request and response

#### File Organization

```
message/services/
├── common/
│   ├── status.fbs          # Common ServiceStatus enum, ErrorInfo
│   └── types.fbs           # Shared primitive types
├── navigation.fbs          # Navigation-related services
├── perception.fbs          # Perception-related services
├── control.fbs             # Control-related services
└── diagnostics.fbs         # Diagnostics-related services
```

### 1.2 Required Message Fields

#### All Requests and Responses MUST Include:

```fbs
table {ServiceName}Request {
  aorta_header: aorta.sys.AortaHeader (required);  // Timestamp, sender_id, etc.
  // ... service-specific fields
}

table {ServiceName}Response {
  aorta_header: aorta.sys.AortaHeader (required);  // Timestamp, responder_id
  status: aorta.services.common.ServiceStatus (required);  // SUCCESS, ERROR, etc.
  // ... service-specific fields
}
```

**Rationale:**
- `aorta_header`: Enables tracing, logging, and debugging across distributed systems
- `status`: Standardized error handling and success indication

> **Scope note:** the "MUST Include" above is a **typed-schema authoring
> convention**, not a wire requirement. Aorta Core never parses or enforces
> request/response payload shape on the RPC data path; raw-byte services
> (Q2 below) and foreign native-Zenoh participants legitimately carry
> payloads with no `aorta_header`/`status` at all. The rule binds authors of
> FlatBuffers service schemas in this repo, so that all typed services get
> uniform tracing and error semantics.

#### Optional Error Reporting

```fbs
table {ServiceName}Response {
  aorta_header: aorta.sys.AortaHeader (required);
  status: aorta.services.common.ServiceStatus (required);
  error_info: aorta.services.common.ErrorInfo;  // Optional detailed error
  // ... service-specific fields
}
```

### 1.3 Schema Identification

Each FlatBuffers schema is identified by:

```rust
// SHA-256 hash of the binary FlatBuffers schema (.bfbs)
pub type SchemaHash = [u8; 32];  // 32-byte binary
pub type SchemaHashHex = String; // 64-character hex string
```

**Example:**
```
Request Hash:  a7f3d2e1c4b8... (64-char hex)
Response Hash: 3f8a9c2d5e7b... (64-char hex)
```

### 1.4 Namespace Conventions

```fbs
namespace aorta.services.navigation;  // Domain-based namespace

// Import common definitions
include "aorta_base.fbs";                        // AortaHeader
include "services/common/status.fbs";            // ServiceStatus, ErrorInfo
```

---

## 2. Schema Registration & Discovery

### 2.1 Registration Mechanism

Services register **TWO** schemas (request + response) via the `schema_by_key` Queryable:

#### Key Format

```
aorta/{group}/sys/schema_by_key/srv/{service_name}
```

**Examples:**
```
aorta/robot_01/sys/schema_by_key/srv/get_plan
aorta/robot_01/sys/schema_by_key/srv/cancel_plan
aorta/robot_01/sys/schema_by_key/srv/get_current_pose
```

#### Payload Format

```fbs
// From schema_by_key.fbs
table SchemaByKey {
  kind: SchemaKind;  // = Service (1)
  
  // For Services: req_hash + req_bfbs + res_hash + res_bfbs
  schema_hash: [ubyte] (required);  // Request schema hash (32 bytes)
  bfbs: [ubyte] (required);         // Request binary schema
  
  // Service-specific extensions
  res_schema_hash: [ubyte];         // Response schema hash (32 bytes)
  res_bfbs: [ubyte];                // Response binary schema
  
  // Common metadata
  schema_hash_hex: string;          // Request hash as 64-char hex
  res_schema_hash_hex: string;      // Response hash as 64-char hex
  created_at_ns: uint64;            // Registration timestamp
  registration_id: string;          // Unique registration ID
}

enum SchemaKind : ubyte {
  Topic = 0,
  Service = 1,
  Param = 2,
}
```

### 2.2 Service Registration Flow

```rust
// Pseudocode for Service::start()
impl Service {
    pub async fn start(&mut self) -> Result<()> {
        // 1. Compute schema hashes
        let req_hash = sha256(&self.req_bfbs);
        let res_hash = sha256(&self.res_bfbs);
        
        // 2. Check for existing registration
        if let Some(existing) = self.schema_registry
            .fetch_schema_by_key_service(&self.service_name).await? {
            
            // 3. Conflict detection
            if existing.req_hash != req_hash || existing.res_hash != res_hash {
                return self.handle_schema_conflict(existing);
            }
        }
        
        // 4. Register new schema
        self.schema_registry.register_schema_by_key_service(
            &self.service_name,
            req_hash, &self.req_bfbs,
            res_hash, &self.res_bfbs,
        ).await?;
        
        // 5. Start Queryable handler
        self.start_queryable().await
    }
}
```

### 2.3 Client Discovery Flow

```rust
// Pseudocode for Client::call()
impl Client {
    pub async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        // 1. Optional schema discovery
        if self.validate_schema {
            let schema = self.schema_registry
                .fetch_schema_by_key_service(&self.service_name)
                .await?;
            
            // 2. Validate request against schema
            self.validate_request(&request, &schema.req_bfbs)?;
        }
        
        // 3. Send Query
        let reply = self.session.get(&self.selector)
            .timeout(self.timeout)
            .await?;
        
        // 4. Optional response validation
        if self.validate_schema {
            self.validate_response(&reply.payload, &schema.res_bfbs)?;
        }
        
        Ok(reply.payload.to_vec())
    }
}
```

### 2.4 SchemaRegistry API Extensions

```rust
// New struct for Service schema storage
pub struct ServiceSchemaByKeyValue {
    pub req_hash: SchemaHash,
    pub req_bfbs: Vec<u8>,
    pub res_hash: SchemaHash,
    pub res_bfbs: Vec<u8>,
    pub created_at_ns: u64,
    pub registration_id: String,
}

impl SchemaRegistry {
    /// Register a Service schema (req + res pair)
    pub async fn register_schema_by_key_service(
        &self,
        service_name: &str,
        req_hash: SchemaHash,
        req_bfbs: &[u8],
        res_hash: SchemaHash,
        res_bfbs: &[u8],
    ) -> Result<()>;
    
    /// Fetch a Service schema by service name
    pub async fn fetch_schema_by_key_service(
        &self,
        service_name: &str,
    ) -> Result<Option<ServiceSchemaByKeyValue>>;
}
```

---

## 3. Schema Validation & Conflict Handling

### 3.1 Conflict Detection

A **schema conflict** occurs when:

1. **Startup conflict**: Service tries to register schemas that differ from existing registration
2. **Runtime conflict**: Client discovers schema different from expected version

#### Conflict Detection at Service Startup

```rust
// Environment variable controls behavior
match env::var("AORTA_SCHEMA_CONFLICT_ACTION").as_deref() {
    Ok("reject") => {
        // Explicit: Fail fast on schema mismatch
        return Err(AortaError::SchemaConflict {
            service_name: self.service_name.clone(),
            expected_req: hex::encode(&expected.req_hash),
            actual_req: hex::encode(&actual.req_hash),
            expected_res: hex::encode(&expected.res_hash),
            actual_res: hex::encode(&actual.res_hash),
        });
    },
    Ok("warn") | Err(_) => {
        // Default: Log warning and continue with new schema
        warn!("Schema conflict for service '{}': continuing with new schema", 
              self.service_name);
    },
    _ => {
        // Invalid values fall back to warn
        warn!("Invalid AORTA_SCHEMA_CONFLICT_ACTION, continuing with warn");
    },
}
```

### 3.2 Schema Validation Modes

```rust
// Environment variable controls validation strictness
pub enum SchemaValidationMode {
    Off,      // No validation (fastest, default)
    Warn,     // Validate and log warnings on mismatch
    Strict,   // Validate and reject on mismatch (debug only)
}

impl Client {
    fn validation_mode(&self) -> SchemaValidationMode {
        match env::var("AORTA_SCHEMA_VALIDATE").as_deref() {
            Ok("strict") => SchemaValidationMode::Strict,
            Ok("warn") => SchemaValidationMode::Warn,
            Ok("off") | Err(_) => SchemaValidationMode::Off, // Default: no validation
            _ => SchemaValidationMode::Off,
        }
    }
}
```

### 3.4 Hash Mismatch Policy (Updated)

**Default behavior:** log-only at registration and allow runtime execution.
Hash mismatches are recorded for observability but **do not block** startup or calls.
Strict enforcement is only used in tests or explicit debug mode.

### 3.3 Validation Process

```rust
impl Client {
    fn validate_request(&self, data: &[u8], bfbs: &[u8]) -> Result<()> {
        // Use FlatBuffers reflection to verify message structure
        let schema = reflection::get_root_as_schema(bfbs)?;
        
        // Attempt to parse message with schema
        match self.parse_with_schema(data, schema) {
            Ok(_) => Ok(()),
            Err(e) => match self.validation_mode() {
                SchemaValidationMode::Strict => Err(e),
                SchemaValidationMode::Warn => {
                    warn!("Request validation failed: {}", e);
                    Ok(())
                },
                SchemaValidationMode::Off => Ok(()),
            }
        }
    }
}
```

---

## 4. Attachment Policy for RPC

### 4.1 Schema Attachment Options

**Question:** Should schema hash be attached to Query/Reply messages?

| Policy | Pros | Cons | Use Case |
|--------|------|------|----------|
| **Never** | Minimal overhead, relies on `schema_by_key` discovery | Requires discoverable registry | LAN/single-robot |
| **Always** | Self-describing messages, no discovery needed | 64-byte overhead per message | Cloud/multi-robot |
| **OnDemand** | Attach only when Client requests via attachment | Complex negotiation | Hybrid environments |

### 4.2 Current Implementation (Phase 2)

**Default Policy: `Never`**

```rust
impl Service {
    pub async fn handle_query(&self, query: Query) -> Result<()> {
        let request = query.payload().to_vec();
        
        // Process request (no schema validation by default)
        let response = (self.handler)(request).await?;
        
        // Send reply WITHOUT schema attachment
        query.reply(self.key_expr.clone(), response).await?;
        
        Ok(())
    }
}
```

### 4.3 RPC Cancellation (Phase 4+)

Current RPC timeout only stops local waiting and does **not** terminate remote execution.
Long-running tasks should use **Action** semantics (Goal/Cancel + Feedback/Result) to enable
true cancellation. Zenoh query cancellation (1.7.x+) can be used as an optimization layer,
but Action-level cancel remains the primary control path.

**Rationale:**
- Phase 2 targets single-robot/LAN scenarios where `schema_by_key` is always available
- Reduces message overhead (64 bytes saved per message)
- Schema discovery happens once at Client initialization

### 4.3 Future Extension (Phase 3+)

**Attachment Format:**

```fbs
// Future: RpcAttachment in aorta_base.fbs
table RpcAttachment {
  req_schema_hash_hex: string;  // 64-char hex
  res_schema_hash_hex: string;  // 64-char hex
  encoding: string;             // "flatbuffers"
  version: uint32;              // Schema version
}
```

**Attachment via Zenoh:**

```rust
// Attach schema metadata to Query
let attachment = encode_rpc_attachment(req_hash, res_hash);
query.with_attachment(attachment);

// Retrieve in Service handler
if let Some(att) = query.attachment() {
    let schema_info = decode_rpc_attachment(att)?;
    // Optional: validate against registered schema
}
```

---

## 5. Best Practices

### 5.1 Service Definition Checklist

- [ ] Request/Response tables follow `{ServiceName}Request/Response` naming
- [ ] Both messages include `aorta_header: AortaHeader (required)`
- [ ] Response includes `status: ServiceStatus (required)`
- [ ] Response includes optional `error_info: ErrorInfo` for detailed errors
- [ ] Namespace matches domain: `aorta.services.{domain}`
- [ ] File includes necessary imports (`aorta_base.fbs`, `status.fbs`)
- [ ] Service name in key expression matches FlatBuffers table name

### 5.2 Schema Evolution Guidelines

#### Backward-Compatible Changes (Safe)

```fbs
// ✅ SAFE: Adding optional fields
table GetPlanRequest {
  aorta_header: aorta.sys.AortaHeader (required);
  start_pose: Pose (required);
  goal_pose: Pose (required);
  max_planning_time_ms: uint32;  // NEW: optional field
}
```

#### Breaking Changes (Requires New Service)

```fbs
// ❌ BREAKING: Changing field type or removing fields
table GetPlanRequest {
  aorta_header: aorta.sys.AortaHeader (required);
  // start_pose: Pose (required);  // REMOVED - breaking change!
  waypoints: [Pose] (required);    // CHANGED - now array, breaking!
}

// ✅ SOLUTION: Create new service version
table GetPlanV2Request {
  aorta_header: aorta.sys.AortaHeader (required);
  waypoints: [Pose] (required);
}
```

**Versioning Strategy:**
- Append `V2`, `V3` to service name for breaking changes
- Register as separate service: `get_plan_v2`
- Deprecate old service after migration period

### 5.3 Testing Schema Registration

```rust
#[tokio::test]
async fn test_service_schema_registration() {
    let config = AortaConfig::default();
    let session = create_test_session(&config).await.unwrap();
    let schema_registry = SchemaRegistry::new(session.clone(), &config).await.unwrap();
    
    // Embed test schemas
    const REQ_BFBS: &[u8] = include_bytes!("../message/services/navigation_GetPlanRequest.bfbs");
    const RES_BFBS: &[u8] = include_bytes!("../message/services/navigation_GetPlanResponse.bfbs");
    
    // Register service schema
    let req_hash = compute_sha256(REQ_BFBS);
    let res_hash = compute_sha256(RES_BFBS);
    schema_registry.register_schema_by_key_service(
        "get_plan",
        req_hash,
        REQ_BFBS,
        res_hash,
        RES_BFBS,
    ).await.unwrap();
    
    // Fetch and verify
    let fetched = schema_registry
        .fetch_schema_by_key_service("get_plan")
        .await
        .unwrap()
        .expect("Schema should exist");
    
    assert_eq!(fetched.req_hash, req_hash);
    assert_eq!(fetched.res_hash, res_hash);
    assert_eq!(fetched.req_bfbs, REQ_BFBS);
    assert_eq!(fetched.res_bfbs, RES_BFBS);
}
```

### 5.4 Error Handling Best Practices

```fbs
// Define domain-specific error codes
namespace aorta.services.navigation;

enum NavigationErrorCode : uint32 {
  UNKNOWN = 0,
  NO_PATH_FOUND = 1001,
  INVALID_START_POSE = 1002,
  INVALID_GOAL_POSE = 1003,
  PLANNING_TIMEOUT = 1004,
  OBSTACLE_COLLISION = 1005,
}

table GetPlanResponse {
  aorta_header: aorta.sys.AortaHeader (required);
  status: aorta.services.common.ServiceStatus (required);
  error_info: aorta.services.common.ErrorInfo;  // Use common ErrorInfo
  error_code: NavigationErrorCode;              // Domain-specific code
  
  // Success data
  path: Path;
  estimated_duration_ms: uint32;
}
```

```rust
// In Service handler
if no_path_found {
    return Ok(GetPlanResponseBuilder::new(&mut builder)
        .aorta_header(header_offset)
        .status(ServiceStatus::ERROR)
        .error_info(ErrorInfoBuilder::new(&mut builder)
            .message(Some("No valid path found"))
            .code(NavigationErrorCode::NO_PATH_FOUND as u32)
            .finish())
        .finish());
}
```

---

## 6. Example Implementation

### 6.1 Complete Service Definition

**File:** `message/services/navigation.fbs`

```fbs
namespace aorta.services.navigation;

include "aorta_base.fbs";
include "services/common/status.fbs";

// ============================================================================
// GetPlan Service: Compute navigation path
// ============================================================================

table GetPlanRequest {
  aorta_header: aorta.sys.AortaHeader (required);
  start_pose: Pose (required);
  goal_pose: Pose (required);
  max_planning_time_ms: uint32 = 5000;
  allow_unknown_space: bool = false;
}

table GetPlanResponse {
  aorta_header: aorta.sys.AortaHeader (required);
  status: aorta.services.common.ServiceStatus (required);
  error_info: aorta.services.common.ErrorInfo;
  
  path: Path;
  estimated_duration_ms: uint32;
}

// Supporting types
table Pose {
  x: float;
  y: float;
  theta: float;
}

table Path {
  poses: [Pose];
  total_distance: float;
}
```

### 6.2 Rust Service Implementation

```rust
use aorta_core::{Service, SchemaRegistry, AortaConfig};
use sha2::{Sha256, Digest};

// Embed compiled schemas at build time
const GET_PLAN_REQ_BFBS: &[u8] = include_bytes!(
    "../message/services/navigation_GetPlanRequest.bfbs"
);
const GET_PLAN_RES_BFBS: &[u8] = include_bytes!(
    "../message/services/navigation_GetPlanResponse.bfbs"
);

#[tokio::main]
async fn main() -> Result<()> {
    let config = AortaConfig::default();
    let session = zenoh::open(config.to_zenoh_config()).await?;
    let schema_registry = SchemaRegistry::new(session.clone(), &config).await?;
    
    // Create Service with schema information
    let mut service = Service::builder()
        .session(session.clone())
        .service_name("get_plan")
        .group("robot_01")
        .handler(Box::new(handle_get_plan))
        .schema_registry(schema_registry.clone())
        .req_schema_bfbs(GET_PLAN_REQ_BFBS.to_vec())
        .res_schema_bfbs(GET_PLAN_RES_BFBS.to_vec())
        .build()?;
    
    // Start service (automatically registers schemas)
    service.start().await?;
    
    info!("GetPlan service started on aorta/robot_01/srv/get_plan");
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn handle_get_plan(request: Vec<u8>) -> Result<Vec<u8>> {
    // Decode request
    let req = flatbuffers::root::<GetPlanRequest>(&request)?;
    
    // Business logic
    let path = compute_path(req.start_pose(), req.goal_pose())?;
    
    // Build response
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let response = GetPlanResponse::create(&mut builder, &GetPlanResponseArgs {
        aorta_header: Some(create_header(&mut builder)),
        status: ServiceStatus::SUCCESS,
        path: Some(path),
        estimated_duration_ms: 12345,
        ..Default::default()
    });
    builder.finish(response, None);
    
    Ok(builder.finished_data().to_vec())
}
```

### 6.3 Rust Client Implementation

```rust
use aorta_core::{Client, SchemaRegistry};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AortaConfig::default();
    let session = zenoh::open(config.to_zenoh_config()).await?;
    let schema_registry = SchemaRegistry::new(session.clone(), &config).await?;
    
    // Create Client with schema discovery
    let client = Client::builder()
        .session(session.clone())
        .service_name("get_plan")
        .group("robot_01")
        .timeout(Duration::from_secs(5))
        .schema_registry(schema_registry.clone())
        .validate_schema(true)  // Enable schema validation
        .build()?;
    
    // Build request
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let request = GetPlanRequest::create(&mut builder, &GetPlanRequestArgs {
        aorta_header: Some(create_header(&mut builder)),
        start_pose: Some(Pose::new(0.0, 0.0, 0.0)),
        goal_pose: Some(Pose::new(10.0, 10.0, 1.57)),
        max_planning_time_ms: 5000,
        ..Default::default()
    });
    builder.finish(request, None);
    
    // Call service
    let response_bytes = client.call(builder.finished_data()).await?;
    
    // Decode response
    let response = flatbuffers::root::<GetPlanResponse>(&response_bytes)?;
    
    match response.status() {
        ServiceStatus::SUCCESS => {
            println!("Path computed: {} waypoints", response.path().unwrap().poses().len());
        },
        ServiceStatus::ERROR => {
            eprintln!("Error: {}", response.error_info().unwrap().message());
        },
        _ => {},
    }
    
    Ok(())
}
```

---

## 7. FAQ

### Q1: Why register both request AND response schemas?

**A:** This enables:
1. **Client-side validation**: Clients can validate requests before sending
2. **Service-side validation**: Services can validate responses before replying
3. **Introspection tools**: Tools can display full RPC signature: `GetPlan(Request) -> Response`
4. **Code generation**: FFI/SDK generators need both schemas to create type-safe wrappers

### Q2: What happens if a Service doesn't register its schema?

**A:** The Service will still function (schema registration is optional), but:
- ❌ No automatic schema discovery for Clients
- ❌ No conflict detection across Service restarts
- ❌ Introspection tools cannot display service information
- ✅ **Recommendation**: Always register schemas unless using raw byte payloads

### Q3: Can multiple Services register the same schema?

**A:** Yes! If two Services (e.g., `robot_01/srv/get_plan` and `robot_02/srv/get_plan`) use identical request/response schemas, they will:
- Compute the same schema hashes
- Register at different key expressions (different groups)
- No conflict occurs

### Q4: How to handle schema evolution?

**A:** Follow the compatibility matrix:

| Change Type | Example | Compatible? | Action |
|-------------|---------|-------------|--------|
| Add optional field | `new_field: uint32;` | ✅ Yes | Update schema, re-register |
| Remove optional field | Remove unused field | ⚠️ Maybe | Test with old clients |
| Change field type | `uint32` → `uint64` | ❌ No | Create new service version |
| Rename field | `pose` → `position` | ❌ No | Create new service version |

**Best practice:** Use `V2`, `V3` suffixes for breaking changes.

### Q5: Can I use schema validation in production?

**A:** Yes, but consider performance:

| Mode | Overhead | Use Case |
|------|----------|----------|
| `AORTA_SCHEMA_VALIDATE=off` | ~0μs | Production (trusted environment) |
| `AORTA_SCHEMA_VALIDATE=warn` | ~10-50μs | Staging (detect issues) |
| `AORTA_SCHEMA_VALIDATE=strict` | ~10-50μs | Development (enforce correctness) |

**Recommendation:** Use `strict` in development, `off` in production.

### Q6: How large are BFBS files?

**A:** Typical sizes:

| Schema Complexity | BFBS Size | Example |
|-------------------|-----------|---------|
| Simple (2-3 fields) | ~200-500 bytes | `CancelPlanRequest` |
| Medium (5-10 fields) | ~500-2KB | `GetPlanRequest` |
| Complex (nested tables) | ~2-10KB | `PerceptionResultResponse` |

**Total overhead per Service registration:** ~1-20KB (depends on schema complexity)

### Q7: What if `schema_by_key` Queryable is unavailable?

**A:** Behavior depends on configuration:

```rust
// If SchemaRegistry cannot start its Queryable
match config.schema_registry_mode {
    SchemaRegistryMode::Required => {
        // Service.start() fails immediately
        Err(AortaError::SchemaRegistryUnavailable)
    },
    SchemaRegistryMode::Optional => {
        // Service starts without schema registration
        warn!("SchemaRegistry unavailable, continuing without schema");
        Ok(())
    },
}
```

**Default:** `Optional` (fail-safe mode)

### Q8: Can I query all registered services?

**A:** Not via `schema_by_key` wildcard scans anymore.

`schema_by_key` is now intended for **exact-key lookup** of a known service:

```rust
let reply = session
    .get("aorta/robot_01/sys/schema_by_key/srv/get_plan")
    .await?;
```

For **service discovery** and **introspection tooling**, use admin/resource inventory or other explicit discovery sources first, then resolve schema with an exact `schema_by_key` query for the selected service.

---

## Appendix A: Complete File Reference

### Schema Files
- `message/schemas/system/schema_by_key.fbs` - Schema registration format
- `message/schemas/system/aorta_base.fbs` - AortaHeader definition
- `message/schemas/shared/service_common/status.fbs` - ServiceStatus enum
- `message/schemas/service/example/navigation.fbs` - Navigation service examples

### Core Implementation
- `core/src/schema_registry.rs` - SchemaRegistry implementation
- `core/src/service.rs` - Service (RPC server) implementation
- `core/src/client.rs` - Client (RPC caller) implementation
- `core/src/schema_by_key_fb.rs` - SchemaByKey decoder

### Tests
- `core/test/service_client_integration_tests.rs` - Service/Client integration tests

---

**End of Document**
