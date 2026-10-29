# Aorta Router ACL Minimal Samples

Last updated: 2026-02-10

## Scope

This document provides minimal ACL samples for schema-related key expressions in Aorta deployments:

- Global schema-by-hash: `aorta/sys/schema/**`
- Group-scoped schema-by-key: `aorta/**/sys/schema_by_key/**`

The goal is to prevent unintended cross-domain schema propagation while keeping required query paths available.

## Design Rules

1. Default-deny schema key expressions across domains.
2. Open only the exact prefixes required by your external communication surface.
3. If external clients resolve attachment hash (`schema_hash_hex`), you must allow `aorta/sys/schema/**` in addition to `schema_by_key`.
4. In shared namespace deployments, restrict by connection direction and namespace first, then key expression.

## Minimal Policy Patterns

The snippets below are intentionally policy-pattern examples. Adapt field names and nesting to your Zenoh router version.

### Pattern A: Single robot / local namespace

Use when all producers/consumers are inside the same trusted namespace.

```json5
{
  acl: {
    default: "deny",
    rules: [
      // Internal schema query paths only
      { action: "allow", key_expr: "aorta/sys/schema/**", direction: ["ingress", "egress"], peers: ["local_namespace"] },
      { action: "allow", key_expr: "aorta/**/sys/schema_by_key/**", direction: ["ingress", "egress"], peers: ["local_namespace"] },
    ]
  }
}
```

### Pattern B: Robot to cloud bridge (minimal allowlist)

Use when cloud tools need schema lookup for a subset of external interfaces.

```json5
{
  acl: {
    default: "deny",
    rules: [
      // Keep schema traffic blocked by default across the bridge
      { action: "deny", key_expr: "aorta/sys/schema/**", direction: ["egress"], peers: ["cloud_link"] },
      { action: "deny", key_expr: "aorta/**/sys/schema_by_key/**", direction: ["egress"], peers: ["cloud_link"] },

      // Allow only required external surface
      { action: "allow", key_expr: "aorta/sys/schema/**", direction: ["egress"], peers: ["cloud_link"], when: "external_topics_only" },
      { action: "allow", key_expr: "aorta/**/sys/schema_by_key/pub/external/**", direction: ["egress"], peers: ["cloud_link"] },
      { action: "allow", key_expr: "aorta/**/sys/schema_by_key/srv/external/**", direction: ["egress"], peers: ["cloud_link"] },
    ]
  }
}
```

### Pattern C: Multi-robot shared namespace

Use when multiple robots share one namespace and must avoid schema fan-out.

```json5
{
  acl: {
    default: "deny",
    rules: [
      // Deny cross-robot schema exchange
      { action: "deny", key_expr: "aorta/sys/schema/**", peers: ["robot_* != self"] },
      { action: "deny", key_expr: "aorta/**/sys/schema_by_key/**", peers: ["robot_* != self"] },

      // Optional: allow a centralized governance/observability node
      { action: "allow", key_expr: "aorta/sys/schema/**", peers: ["schema_gateway"] },
      { action: "allow", key_expr: "aorta/**/sys/schema_by_key/**", peers: ["schema_gateway"] },
    ]
  }
}
```

## Verification Checklist

1. `schema_by_key` works for intended local topics/services.
2. `aorta/sys/schema/{hash}` is reachable where attachment-based decoding is required.
3. Cross-domain peers cannot query unrelated schema keys.
4. Expected deny events are visible in router logs/metrics.
