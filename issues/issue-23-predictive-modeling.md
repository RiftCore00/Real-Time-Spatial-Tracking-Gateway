## Title
Implement predictive location modeling with Kalman filtering, trajectory extrapolation, and anomaly detection for sub-second ETA and geofence pre-alerts

## Difficulty
10/10 — Expert. Estimated effort: 6–8 days for a senior engineer.

## Context
The `README.md` targets "low-latency, high-throughput telemetry scenarios such as fleet tracking, asset monitoring, geofencing enforcement, and live mapping" (line 25). Current system is purely reactive — broadcasts location *after* received. For proactive operations (ETA to depot, geofence breach prediction, collision warning), the gateway must **predict** near-future positions from noisy GPS streams. A Kalman filter fusing position, velocity, and heading can predict 30–60 seconds ahead with <10m error for highway-speed vehicles.

## Problem statement
Design and implement a predictive modeling engine that:

1. **Per-client Kalman filter**: For each tracked asset, maintain a constant-velocity (CV) or constant-turn-rate (CTRV) Kalman filter state `{ x, y, vx, vy, heading, headingRate }` in local tangent plane (ENU coordinates). Update on each `location_update` (measurement: lat, lon, speed, heading). Predict `predict(dt)` → future position at `now + dt`.

2. **Coordinate transformation**: GPS (WGS84 lat/lon) → local ENU (meters) centered on filter origin. Use `origin = first valid fix`. Predict in ENU, transform back to lat/lon for output. Handle origin drift for long tracks (re-origin every 50km).

3. **Trajectory extrapolation API**: `predictor.getTrajectory(clientId, horizons: [10, 30, 60] seconds)` → `{ horizon, lat, lon, speed, heading, confidenceEllipse }`. Confidence ellipse from filter covariance `P` (2D position submatrix).

4. **Geofence pre-alerts**: For each predicted point, check geofence engine (issue 12) — if trajectory intersects fence within `preAlertHorizon` (default 60s), emit `geofence_pre_alert` event: `{ fenceId, fenceName, predictedEntryTime, predictedEntryPoint, confidence }`. Allows dispatchers to reroute *before* breach.

5. **Anomaly detection**: 
   - **Measurement anomaly**: Innovation (residual) `z - Hx` exceeds 3σ → flag `gps_anomaly` (multipath, spoofing, dropout).
   - **Behavioral anomaly**: Speed/heading change exceeds physical limits (e.g., 0→100 km/h in 1s) → flag `kinematic_anomaly`.
   - **Predictive anomaly**: Predicted position diverges from actual by > 3σ after 30s → flag `model_divergence` (filter needs reset).

6. **ETA computation**: For a target (depot, customer site, next waypoint), compute ETA distribution from predicted trajectory + covariance. `predictor.getETA(clientId, targetLat, targetLon)` → `{ etaMean, etaStdDev, arrivalProbabilityAt(t) }`.

7. **Integration with broadcast**: Predictive events (`geofence_pre_alert`, `gps_anomaly`, `eta_update`) are new message types broadcast to room (sequenced, replayable per issue 6/16).

8. **Configuration**: `PREDICTOR_ENABLE: true`, `PREDICTOR_MODEL: "CV" | "CTRV"`, `PREDICTOR_PROCESS_NOISE: 0.1`, `PREDICTOR_MEASUREMENT_NOISE: 5.0` (meters), `PREDICTOR_PRE_ALERT_HORIZON_S: 60`, `PREDICTOR_MAX_HORIZON_S: 120`.

## Current behavior
- No predictive modeling.
- `geofence-engine.js` (issue 12) only evaluates current position.
- No Kalman filter, no anomaly detection, no ETA.

## Required behavior
- New module `src/predictor.js` exporting `PredictiveEngine` class.
- `PredictiveEngine` constructor: `{ geofenceEngine, roomManager, config }`.
- `predictor.update(clientId, location)` — runs Kalman filter predict+update cycle.
- `predictor.getTrajectory(clientId, horizons)` — returns predicted positions with confidence.
- `predictor.getETA(clientId, targetLat, targetLon)` — returns ETA distribution.
- `predictor.checkPreAlerts(clientId)` — evaluates predicted trajectory against geofences, emits pre-alerts.
- `predictor.detectAnomalies(clientId, location)` — returns anomaly flags.
- Kalman filter implementation (CV model):
  - State: `x = [x, y, vx, vy]^T` (ENU meters, m/s)
  - Transition: `F = [[1, 0, dt, 0], [0, 1, 0, dt], [0, 0, 1, 0], [0, 0, 0, 1]]`
  - Measurement: `H = [[1, 0, 0, 0], [0, 1, 0, 0]]` (position only)
  - Process noise `Q`, measurement noise `R` — configurable.
  - Standard predict/update equations.
- CTRV model (optional): adds heading and heading rate.
- ENU transform: `latLonToEnu(lat, lon, originLat, originLon)` and inverse.
- Memory: one filter per active client. Cleanup on disconnect (TTL 1 hour).

## Constraints
- Do not modify `auth.js`, `validator.js`, `rate-limiter.js`, `conn-rate-limiter.js`, `logger.js`, `errors.js`, `room-manager.js`, `geofence-engine.js`, `protocol-registry.js`, `distributed-room-manager.js`, `tls-manager.js`, `admin-server.js`, `session-manager.js`, `compression.js`, `topology-manager.js`, `event-sourcing.js`, `collaborative-editor.js`.
- Do not modify existing test files. New test files required.
- No new npm dependencies — implement Kalman filter from scratch (~100 lines).
- Filter must be numerically stable (use Joseph form for covariance update).
- Prediction horizon max 120s — beyond that, uncertainty too high.
- Pre-alerts must not spam: debounce per `(clientId, fenceId)` — only alert on first prediction of entry.
- Anomaly events rate-limited to 1/min per client per anomaly type.
- Works with distributed mode (issue 11) — filter state synced via session resumption (issue 18).

## Acceptance criteria
- [ ] Kalman filter converges: stationary vehicle → position uncertainty decreases over time
- [ ] Moving vehicle at 30 m/s → 30s prediction error < 10m (simulated GPS noise σ=5m)
- [ ] `getTrajectory` returns correct ENU→lat/lon transform
- [ ] Geofence pre-alert emitted 60s before predicted entry
- [ ] GPS anomaly detected when innovation > 3σ
- [ ] Kinematic anomaly detected for impossible acceleration
- [ ] ETA distribution mean ± stddev matches Monte Carlo simulation
- [ ] Filter state saved/restored via session resumption (issue 18)
- [ ] `npm run lint` passes
- [ ] All existing tests pass
- [ ] New test file: `tests/predictor.test.js` with filter accuracy tests, pre-alert scenarios, anomaly detection, ETA validation

## Out of scope
- Multi-model IMM (Interacting Multiple Model) — single CV/CTRV model sufficient.
- Map-matching / road network constraints — free-space prediction only.
- Long-term prediction (>2 min) — uncertainty unbounded.
- Client-side prediction — server-only for this issue.

## Hints and references
- Kalman filter (Joseph form for numerical stability):
  ```js
  // Predict
  x = F @ x
  P = F @ P @ F.T + Q
  // Update
  y = z - H @ x
  S = H @ P @ H.T + R
  K = P @ H.T @ inv(S)
  x = x + K @ y
  P = (I - K @ H) @ P @ (I - K @ H).T + K @ R @ K.T  // Joseph form
  ```
- ENU transformation (origin at lat0, lon0):
  ```js
  const R = 6371000; // Earth radius m
  function latLonToEnu(lat, lon, lat0, lon0) {
    const dLat = (lat - lat0) * Math.PI / 180;
    const dLon = (lon - lon0) * Math.PI / 180;
    const x = R * dLon * Math.cos(lat0 * Math.PI / 180);
    const y = R * dLat;
    return { x, y };
  }
  function enuToLatLon(x, y, lat0, lon0) {
    const lat = lat0 + y / R * 180 / Math.PI;
    const lon = lon0 + x / (R * Math.cos(lat0 * Math.PI / 180)) * 180 / Math.PI;
    return { lat, lon };
  }
  ```
- Confidence ellipse: eigenvalues of 2x2 position covariance submatrix → semi-major/minor axes, orientation.
- Geofence intersection with predicted trajectory: sample predicted positions at 10s intervals, check point-in-polygon (issue 12). For linear motion, can compute exact intersection time with polygon edges.
- Integration point: in `server.js` `location_update` case, after geofence processing, call `predictor.update(actualClientId, { lat, lon, speed, heading, timestamp })` and `predictor.checkPreAlerts(actualClientId)`.