## Title
Implement TLS termination with automated certificate rotation, mTLS client certificate validation, and SNI-based multi-tenant certificate selection

## Difficulty
10/10 — Expert. Estimated effort: 4–6 days for a senior engineer.

## Context
The `README.md` states "all communication runs over `wss://` in production" (line 34) and the architecture diagram shows `wss://` between clients and the gateway. However, the codebase has zero TLS support: `src/server.js` creates a plain `http.createServer()` (line 26) and `new WebSocketServer({ server })` (line 46–49). The `Dockerfile` and `docker-compose.yml` expose port 8080 with no TLS termination. There is no certificate loading, no HTTPS server, no mTLS, and no integration with certificate managers (Let's Encrypt, cert-manager, HashiCorp Vault).

In a production fleet-tracking deployment, the gateway must:
- Terminate TLS for `wss://` connections (server certificate).
- Optionally validate client certificates (mTLS) for device authentication — stronger than JWT.
- Support multiple tenants on the same gateway with different certificates (SNI).
- Rotate certificates automatically without downtime (Let's Encrypt 90-day expiry, or internal PKI 24h expiry).
- Support certificate revocation checking (CRL/OCSP) for mTLS.

## Problem statement
Implement a production-grade TLS layer that:

1. **HTTPS/WSS server**: Replace `http.createServer()` with `https.createServer({ cert, key, ca, ... })` in `src/server.js`. The WebSocket server upgrades on top of HTTPS.

2. **Certificate management**:
   - Load certificates from: filesystem (`TLS_CERT_PATH`, `TLS_KEY_PATH`, `TLS_CA_PATH`), environment variables (`TLS_CERT_PEM`, `TLS_KEY_PEM`), or a certificate manager plugin interface.
   - Support multiple certificates with SNI callback: `server.addContext(hostname, { cert, key, ca })` for multi-tenant deployments.
   - Automated rotation: watch certificate files for changes (fs.watch) or poll certificate manager API. On cert change, call `server.setSecureContext(newContext)` — Node.js 18+ supports this for live rotation without restart.

3. **mTLS client certificate validation**:
   - When `TLS_REQUEST_CERT=true` and `TLS_CA_PEM`/`TLS_CA_PATH` set, request client cert: `requestCert: true, rejectUnauthorized: false` (we validate manually for custom logic).
   - On connection, extract `socket.getPeerCertificate(true)` — verify chain against CA, check expiry, check revocation (CRL/OCSP), extract subject CN/SAN as `deviceId`.
   - Map `deviceId` → `clientId` for auth. If mTLS succeeds, JWT token becomes optional (configurable: `TLS_MTLS_REQUIRES_JWT=false`).
   - Revocation: load CRL from `TLS_CRL_PATH` or `TLS_CRL_URL` (periodic fetch). OCSP: use `node:crypto` `verify` with OCSP stapling or external OCSP responder.

4. **Certificate transparency and monitoring**:
   - Expose `/admin/v1/tls/certificates` (issue 14) listing all loaded certs with `{ subject, issuer, validFrom, validTo, san, ocspStatus }`.
   - Metrics: `tls_cert_expiry_days{subject="..."}`, `tls_mtls_connections_total{result="success|revoked|expired|invalid"}`.
   - Alert when any cert expires within 30 days (log warning, admin event).

5. **Integration with auth system (issue 7)**: mTLS device identity can be used as the primary auth factor. JWT becomes a secondary factor or is omitted entirely for mTLS-authenticated connections.

6. **Graceful certificate rotation**: When a new cert is loaded, existing TLS connections continue with old cert. New connections use new cert. No connection drops.

## Current behavior
- `src/server.js`: plain HTTP server, no TLS.
- `src/auth.js`: only JWT, no certificate validation.
- No certificate files, no `https` module usage, no SNI handling.
- `package.json`: no TLS-related dependencies.

## Required behavior
- New module `src/tls-manager.js` exporting `TLSManager` class.
- `TLSManager` constructor: `{ certSources: [{ type: "file"|"env"|"plugin", config }], caSources: [...], crlSources: [...], requestCert: boolean, rejectUnauthorized: false, sniCallback: (hostname) => SecureContext }`.
- `tlsManager.createSecureContext()` returns `tls.SecureContext` for `https.createServer()`.
- `tlsManager.getPeerIdentity(socket)` returns `{ deviceId, subject, issuer, validFrom, validTo, verified: boolean, revocationChecked: boolean }` or throws.
- `tlsManager.watchAndRotate()` starts file watchers / polling. On change, calls `server.setSecureContext(newContext)` and emits `certificate_rotated` event.
- `src/server.js` uses `TLSManager` to create HTTPS server. Connection handler calls `tlsManager.getPeerIdentity(ws._socket)` and passes result to `verifyConnection()` (which now accepts `{ token?, mtlsIdentity? }`).
- `verifyConnection` (issue 7) extended: if `mtlsIdentity.verified && !mtlsIdentity.revoked`, accept with `clientId = mtlsIdentity.deviceId` (or JWT `sub` if both present).
- Admin API (issue 14) endpoints for cert inspection and manual rotation trigger.

## Constraints
- Do not modify `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`.
- Do not modify existing test files. New test files required.
- Use only Node.js built-in `tls`, `https`, `crypto`, `fs` modules. No new npm dependencies for core TLS (certificate manager plugins can be separate packages).
- Certificate rotation must not drop existing connections — `server.setSecureContext()` achieves this in Node 18+.
- mTLS validation must be fast: <5ms per connection for cert chain verification + CRL check (cache CRL in memory, refresh every 5 min).
- SNI callback must be synchronous and fast — cert lookup from in-memory Map.
- Support both PEM (string) and file paths for cert/key/CA/CRL.
- The WebSocket server creation in `server.js` must be refactored to accept a pre-created `https.Server` instance (or `TLSManager` creates it).

## Acceptance criteria
- [ ] `TLSManager` loads cert/key from file paths and creates `SecureContext`
- [ ] HTTPS server starts on `TLS_PORT` (default 8443), WebSocket upgrades work over `wss://`
- [ ] SNI: two certs loaded for `tenant1.example.com` and `tenant2.example.com` — correct cert served based on `ServerName`
- [ ] mTLS: client with valid cert signed by CA connects → `getPeerIdentity` returns verified identity, JWT optional
- [ ] mTLS: client with expired cert → rejected with close code 4003 "Client certificate expired"
- [ ] mTLS: client with revoked cert (in CRL) → rejected with close code 4004 "Client certificate revoked"
- [ ] Certificate rotation: modify cert file, `TLSManager` detects change, calls `server.setSecureContext()`, new connections use new cert, old connections unaffected
- [ ] Metrics: `tls_cert_expiry_days` gauge, `tls_mtls_connections_total` counter
- [ ] Admin API: `GET /admin/v1/tls/certificates` returns cert details
- [ ] `npm run lint` passes
- [ ] All existing tests pass (they use plain HTTP — ensure non-TLS mode still works when `TLS_CERT_PATH` not set)
- [ ] New test file: `tests/tls-manager.test.js` with unit tests for cert loading, SNI, mTLS validation, rotation
- [ ] New test file: `tests/tls-integration.test.js` (requires cert generation) — skipped if `openssl` not available

## Out of scope
- Certificate manager plugins (ACME/Let's Encrypt, cert-manager, Vault) — interface only, implementations are separate.
- OCSP stapling — CRL is sufficient for this issue.
- Client certificate authentication for the admin API (issue 14 uses API keys).
- TLS 1.3 specific features (0-RTT, etc.) — standard TLS 1.2+ is fine.
- Hardware security modules (HSM) — file-based certs only.

## Hints and references
- Node.js `https.createServer({ SNICallback: (hostname, cb) => { cb(null, tlsManager.getContextForHostname(hostname)); } })`.
- `server.setSecureContext(context)` — available in Node 18+. For older Node, you must recreate the server (not required — assume Node 18+).
- Certificate chain verification: `crypto.X509Certificate` (Node 15+) for parsing, `crypto.verify` for signature verification. Or use `openssl` CLI via `child_process` for complex validation (CRL, OCSP).
- CRL parsing: `crypto.X509Certificate` doesn't parse CRL. Use `openssl crl -inform PEM -text -noout` and parse output, or implement minimal CRL parsing (it's just a list of serial numbers).
- For mTLS deviceId extraction: `cert.subject` contains `CN=device-123` or `subjectaltname` has `DNS:device-123`. Parse with regex or `x509` library.
- `ws._socket` is the underlying `tls.TLSSocket` — `ws._socket.getPeerCertificate(true)` returns full chain.
- Integration point in `server.js`:
  ```js
  const tlsManager = new TLSManager(config);
  const httpsServer = https.createServer(tlsManager.createServerOptions(), (req, res) => { ... });
  const wss = new WebSocketServer({ server: httpsServer });
  tlsManager.watchAndRotate(httpsServer);
  ```
- For testing: generate self-signed CA + server cert + client cert with `openssl` in test setup. Use `mkcert` if available for trusted certs.