# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately using GitHub Private
Vulnerability Reporting: open this repository's **Security** page and select
**Report a vulnerability**. If that channel is unavailable, email
vbot-edu@vbot.cn. Do not open a public issue for an undisclosed vulnerability.

We will acknowledge reports within 5 business days and coordinate a fix and
disclosure timeline with the reporter.

## Supported versions

Only the latest published release of this repository receives security
fixes. Older tags are immutable historical snapshots and are not patched.

## Known security characteristics

- Aorta is designed for deployment on trusted, robot-internal networks.
  The default transport does not itself provide authentication or
  encryption; deployments that cross untrusted networks must provide
  confidentiality and authenticity at the transport or link layer (for
  example TLS/QUIC tunnels between endpoints).
- Message payloads are not encrypted at the schema layer. If an
  application schema carries sensitive values such as credentials or
  tokens, their confidentiality relies entirely on the transport described
  above. Applications must not log such values, and should document the
  sensitivity of those fields in their own schema files.
- Dependency advisories for this repository are tracked through the
  standard GitHub advisory tooling; known-open advisories at release time
  are listed in the release notes when they affect shipped code paths.
