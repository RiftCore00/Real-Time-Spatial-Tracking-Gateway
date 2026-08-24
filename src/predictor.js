/**
 * src/predictor.js
 *
 * Predictive location modeling engine (Issue #248).
 *
 * Implements a Constant-Velocity (CV) Kalman filter in local ENU coordinates
 * per tracked client.  Exposes:
 *   - update(clientId, location)          – predict + update cycle
 *   - getTrajectory(clientId, horizons)   – future positions with confidence
 *   - getETA(clientId, targetLat, targetLon) – ETA distribution
 *   - checkPreAlerts(clientId)            – geofence pre-alert emission
 *   - detectAnomalies(clientId, location) – anomaly flags
 *   - removeClient(clientId)              – explicit cleanup
 *
 * No external dependencies – Kalman math implemented from scratch.
 */

import { logger } from "./logger.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EARTH_RADIUS_M = 6_371_000;
const DEG_TO_RAD = Math.PI / 180;
const RAD_TO_DEG = 180 / Math.PI;

/** Clients whose filter has not been touched for this long are evicted. */
const DEFAULT_TTL_MS = 60 * 60 * 1000; // 1 hour

/** Origin is re-established when the vehicle has moved this far from it. */
const ORIGIN_DRIFT_THRESHOLD_M = 50_000; // 50 km

/** Debounce window: don't re-alert the same (clientId, fenceId) pair. */
const PRE_ALERT_DEBOUNCE_MS = 60_000;

/** Anomaly rate-limit: emit at most 1 event per type per client per minute. */
const ANOMALY_RATE_LIMIT_MS = 60_000;

/** Sigma multiplier for anomaly thresholds. */
const ANOMALY_SIGMA = 3;

/** Max speed in m/s considered physically possible (≈ 360 km/h). */
const MAX_PHYSICAL_SPEED_MS = 100;

/** Max acceleration in m/s² considered physically possible for road vehicles (≈2g). */
const MAX_PHYSICAL_ACCEL_MS2 = 20;

/** Max heading rate in rad/s considered physically possible. */
const MAX_PHYSICAL_HEADING_RATE = (Math.PI * 2) / 3; // 120°/s

/** Minimum dt to apply a Kalman predict (avoids divide-by-zero). */
const MIN_DT_S = 0.01;

/** Interval used when sampling trajectory for geofence checks (seconds). */
const TRAJECTORY_SAMPLE_INTERVAL_S = 10;

// ---------------------------------------------------------------------------
// 4×4 matrix helpers (row-major, flat arrays of length 16)
// ---------------------------------------------------------------------------

/** 4×4 identity matrix. */
function mat4Identity() {
  return [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1];
}

/**
 * 4×4 × 4×4 matrix multiplication.
 * @param {number[]} A
 * @param {number[]} B
 * @returns {number[]}
 */
function mat4Mul(A, B) {
  const C = new Array(16).fill(0);
  for (let i = 0; i < 4; i++) {
    for (let j = 0; j < 4; j++) {
      let sum = 0;
      for (let k = 0; k < 4; k++) sum += A[i * 4 + k] * B[k * 4 + j];
      C[i * 4 + j] = sum;
    }
  }
  return C;
}

/**
 * Transpose of a 4×4 matrix.
 * @param {number[]} A
 * @returns {number[]}
 */
function mat4T(A) {
  const T = new Array(16).fill(0);
  for (let i = 0; i < 4; i++)
    for (let j = 0; j < 4; j++) T[j * 4 + i] = A[i * 4 + j];
  return T;
}

/**
 * Add two 4×4 matrices.
 * @param {number[]} A
 * @param {number[]} B
 * @returns {number[]}
 */
function mat4Add(A, B) {
  return A.map((v, i) => v + B[i]);
}

/**
 * Multiply 4×4 matrix by a 4-element column vector.
 * @param {number[]} M
 * @param {number[]} v
 * @returns {number[]}
 */
function mat4MulVec(M, v) {
  return [
    M[0] * v[0] + M[1] * v[1] + M[2] * v[2] + M[3] * v[3],
    M[4] * v[0] + M[5] * v[1] + M[6] * v[2] + M[7] * v[3],
    M[8] * v[0] + M[9] * v[1] + M[10] * v[2] + M[11] * v[3],
    M[12] * v[0] + M[13] * v[1] + M[14] * v[2] + M[15] * v[3],
  ];
}

// ---------------------------------------------------------------------------
// 2×2 matrix helpers (for S and confidence ellipse)
// ---------------------------------------------------------------------------

/**
 * Invert a 2×2 matrix [a,b,c,d] → 1/det * [d,-b,-c,a].
 * @param {number[]} M   flat [a,b,c,d]
 * @returns {number[]|null}
 */
function mat2Inv(M) {
  const det = M[0] * M[3] - M[1] * M[2];
  if (Math.abs(det) < 1e-12) return null;
  return [M[3] / det, -M[1] / det, -M[2] / det, M[0] / det];
}

/**
 * Eigenvalues of a symmetric 2×2 matrix.
 * @param {number[]} M  flat [a,b,b,d] (symmetric)
 * @returns {{ lambda1: number, lambda2: number }}
 */
function sym2Eigen(M) {
  const a = M[0], b = M[1], d = M[3];
  const mean = (a + d) / 2;
  const disc = Math.sqrt(Math.max(0, ((a - d) / 2) ** 2 + b * b));
  return { lambda1: mean + disc, lambda2: mean - disc };
}

/**
 * Orientation of the major axis (radians) for a symmetric 2×2 matrix.
 * @param {number[]} M
 * @returns {number}
 */
function sym2Angle(M) {
  const a = M[0], b = M[1], d = M[3];
  if (Math.abs(b) < 1e-10 && a >= d) return 0;
  if (Math.abs(b) < 1e-10 && a < d) return Math.PI / 2;
  const mean = (a + d) / 2;
  const disc = Math.sqrt(Math.max(0, ((a - d) / 2) ** 2 + b * b));
  const lambda1 = mean + disc;
  // eigenvector for lambda1: [b, lambda1 - a]
  return Math.atan2(lambda1 - a, b);
}

// ---------------------------------------------------------------------------
// Coordinate transforms
// ---------------------------------------------------------------------------

/**
 * WGS84 lat/lon → local ENU in metres, relative to (lat0, lon0).
 * @param {number} lat   degrees
 * @param {number} lon   degrees
 * @param {number} lat0  origin latitude degrees
 * @param {number} lon0  origin longitude degrees
 * @returns {{ x: number, y: number }}  east (x) and north (y) in metres
 */
export function latLonToEnu(lat, lon, lat0, lon0) {
  const dLat = (lat - lat0) * DEG_TO_RAD;
  const dLon = (lon - lon0) * DEG_TO_RAD;
  const x = EARTH_RADIUS_M * dLon * Math.cos(lat0 * DEG_TO_RAD);
  const y = EARTH_RADIUS_M * dLat;
  return { x, y };
}

/**
 * Local ENU metres → WGS84 lat/lon.
 * @param {number} x   east metres
 * @param {number} y   north metres
 * @param {number} lat0 origin latitude degrees
 * @param {number} lon0 origin longitude degrees
 * @returns {{ lat: number, lon: number }}
 */
export function enuToLatLon(x, y, lat0, lon0) {
  const lat = lat0 + (y / EARTH_RADIUS_M) * RAD_TO_DEG;
  const cosLat0 = Math.cos(lat0 * DEG_TO_RAD);
  const lon = lon0 + (x / (EARTH_RADIUS_M * (cosLat0 || 1e-10))) * RAD_TO_DEG;
  return { lat, lon };
}

/**
 * Euclidean distance between two ENU points.
 * @param {number} x1
 * @param {number} y1
 * @param {number} x2
 * @param {number} y2
 * @returns {number}
 */
function enuDistance(x1, y1, x2, y2) {
  return Math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2);
}

// ---------------------------------------------------------------------------
// Kalman filter (CV model, state = [x, y, vx, vy])
// ---------------------------------------------------------------------------

/**
 * Build the process noise matrix Q for a CV model.
 * Uses the standard DWNA (discrete white noise acceleration) model.
 *
 * @param {number} dt          time step in seconds
 * @param {number} sigmaA      acceleration noise std-dev (m/s²)
 * @returns {number[]}  4×4 flat array
 */
function buildQ(dt, sigmaA) {
  const dt2 = dt * dt;
  const dt3 = dt2 * dt;
  const dt4 = dt3 * dt;
  const q = sigmaA * sigmaA;
  return [
    q * dt4 / 4, 0, q * dt3 / 2, 0,
    0, q * dt4 / 4, 0, q * dt3 / 2,
    q * dt3 / 2, 0, q * dt2, 0,
    0, q * dt3 / 2, 0, q * dt2,
  ];
}

/**
 * Build the state-transition matrix F for a CV model.
 *
 * F = [[1, 0, dt, 0],
 *      [0, 1, 0, dt],
 *      [0, 0, 1,  0],
 *      [0, 0, 0,  1]]
 *
 * @param {number} dt
 * @returns {number[]}  4×4 flat array
 */
function buildF(dt) {
  return [
    1, 0, dt, 0,
    0, 1, 0, dt,
    0, 0, 1, 0,
    0, 0, 0, 1,
  ];
}

/**
 * Measurement matrix H (position only).
 * H = [[1, 0, 0, 0],
 *      [0, 1, 0, 0]]
 * Stored as 2-row × 4-col flat array.
 */
const H = [1, 0, 0, 0, 0, 1, 0, 0];

/**
 * Multiply 2×4 H by 4-element state vector → 2-element vector.
 * @param {number[]} x
 * @returns {number[]}
 */
function mulHx(x) {
  return [
    H[0] * x[0] + H[1] * x[1] + H[2] * x[2] + H[3] * x[3],
    H[4] * x[0] + H[5] * x[1] + H[6] * x[2] + H[7] * x[3],
  ];
}

/**
 * Multiply H (2×4) by P (4×4) → 2×4 matrix (flat).
 * @param {number[]} P
 * @returns {number[]}
 */
function mulHP(P) {
  const out = new Array(8).fill(0);
  for (let i = 0; i < 2; i++)
    for (let j = 0; j < 4; j++) {
      let sum = 0;
      for (let k = 0; k < 4; k++) sum += H[i * 4 + k] * P[k * 4 + j];
      out[i * 4 + j] = sum;
    }
  return out;
}

/**
 * Multiply P (4×4) by H^T (4×2) → 4×2 matrix (flat).
 *
 * H = [[1,0,0,0],[0,1,0,0]], so H^T = [[1,0],[0,1],[0,0],[0,0]].
 * P H^T simply selects the first two columns of P.
 *
 * @param {number[]} P
 * @returns {number[]}
 */
function mulPHt(P) {
  // Result is 4×2; column j of result = column j of P (since H^T[:,j] = e_j for j<2)
  const out = new Array(8).fill(0);
  for (let i = 0; i < 4; i++) {
    out[i * 2 + 0] = P[i * 4 + 0]; // P * H^T column 0 = P[:,0]
    out[i * 2 + 1] = P[i * 4 + 1]; // P * H^T column 1 = P[:,1]
  }
  return out;
}

/**
 * S = H P H^T + R  (2×2 innovation covariance).
 * @param {number[]} P   4×4
 * @param {number}   r   measurement noise variance (m²)
 * @returns {number[]}   2×2 flat [s00, s01, s10, s11]
 */
function computeS(P, r) {
  // HP is 2×4, HP*H^T is 2×2
  const HP = mulHP(P);
  // multiply 2×4 HP by H^T (4×2) = 2×2
  const S = new Array(4).fill(0);
  for (let i = 0; i < 2; i++)
    for (let j = 0; j < 2; j++) {
      let sum = 0;
      for (let k = 0; k < 4; k++) sum += HP[i * 4 + k] * H[j * 4 + k]; // H^T[k,j] = H[j,k]
      S[i * 2 + j] = sum;
    }
  S[0] += r;
  S[3] += r;
  return S;
}

/**
 * K = P H^T S^{-1}  (4×2 Kalman gain, flat).
 * @param {number[]} P   4×4
 * @param {number[]} S   2×2
 * @returns {number[]|null}  4×2 or null when S is singular
 */
function computeK(P, S) {
  const PHt = mulPHt(P);
  const Sinv = mat2Inv(S);
  if (!Sinv) return null;
  // K = (4×2) × (2×2) → 4×2
  const K = new Array(8).fill(0);
  for (let i = 0; i < 4; i++)
    for (let j = 0; j < 2; j++) {
      let sum = 0;
      for (let k = 0; k < 2; k++) sum += PHt[i * 2 + k] * Sinv[k * 2 + j];
      K[i * 2 + j] = sum;
    }
  return K;
}

/**
 * Update state: x_new = x + K y  (K is 4×2, y is 2-element vector).
 * @param {number[]} x
 * @param {number[]} K
 * @param {number[]} y
 * @returns {number[]}
 */
function applyKx(x, K, y) {
  return x.map((v, i) => v + K[i * 2] * y[0] + K[i * 2 + 1] * y[1]);
}

/**
 * Joseph-form covariance update for numerical stability:
 *   P_new = (I - KH) P (I - KH)^T + K R K^T
 *
 * @param {number[]} P    4×4
 * @param {number[]} K    4×2
 * @param {number}   r    measurement noise variance
 * @returns {number[]}  4×4
 */
function josephUpdate(P, K, r) {
  // I - KH  (4×4)
  const I = mat4Identity();
  // KH = (4×2)(2×4) = 4×4
  const KH = new Array(16).fill(0);
  for (let i = 0; i < 4; i++)
    for (let j = 0; j < 4; j++) {
      let sum = 0;
      for (let k = 0; k < 2; k++) sum += K[i * 2 + k] * H[k * 4 + j];
      KH[i * 4 + j] = sum;
    }
  const IKH = I.map((v, i) => v - KH[i]);

  // P1 = (I-KH) P (I-KH)^T
  const P1 = mat4Mul(mat4Mul(IKH, P), mat4T(IKH));

  // K R K^T = r * K K^T  (4×2)(2×4) = 4×4
  const KKt = new Array(16).fill(0);
  for (let i = 0; i < 4; i++)
    for (let j = 0; j < 4; j++) {
      let sum = 0;
      for (let k = 0; k < 2; k++) sum += K[i * 2 + k] * K[j * 2 + k];
      KKt[i * 4 + j] = sum;
    }

  return mat4Add(P1, KKt.map((v) => v * r));
}

// ---------------------------------------------------------------------------
// Per-client Kalman filter state
// ---------------------------------------------------------------------------

/**
 * @typedef {Object} FilterState
 * @property {number[]} x          State vector [x, y, vx, vy]
 * @property {number[]} P          4×4 covariance (flat)
 * @property {number}   originLat
 * @property {number}   originLon
 * @property {number}   lastTimestamp  ms since epoch
 * @property {number}   lastActivityMs ms since epoch
 */

/**
 * Create a new filter state seeded from the first valid measurement.
 * @param {number} enuX
 * @param {number} enuY
 * @param {number} vx
 * @param {number} vy
 * @param {number} originLat
 * @param {number} originLon
 * @param {number} timestampMs
 * @param {number} measurementNoiseM  1σ of GPS noise in metres
 * @returns {FilterState}
 */
function initFilter(enuX, enuY, vx, vy, originLat, originLon, timestampMs, measurementNoiseM) {
  const posVar = measurementNoiseM ** 2;
  const velVar = (MAX_PHYSICAL_SPEED_MS / 3) ** 2; // generous initial velocity uncertainty
  return {
    x: [enuX, enuY, vx, vy],
    P: [
      posVar, 0, 0, 0,
      0, posVar, 0, 0,
      0, 0, velVar, 0,
      0, 0, 0, velVar,
    ],
    originLat,
    originLon,
    lastTimestamp: timestampMs,
    lastActivityMs: Date.now(),
  };
}

// ---------------------------------------------------------------------------
// PredictiveEngine
// ---------------------------------------------------------------------------

export class PredictiveEngine {
  /**
   * @param {object} [opts]
   * @param {object|null} [opts.geofenceEngine]          Geofence engine (optional)
   * @param {object|null} [opts.roomManager]             RoomManager (optional)
   * @param {object}      [opts.config]                  Configuration overrides
   * @param {boolean}     [opts.config.enable]
   * @param {string}      [opts.config.model]            "CV" (default)
   * @param {number}      [opts.config.processNoise]     acceleration σ (m/s²)
   * @param {number}      [opts.config.measurementNoise] GPS position σ (m)
   * @param {number}      [opts.config.preAlertHorizonS] seconds ahead to check fences
   * @param {number}      [opts.config.maxHorizonS]      hard cap on prediction horizon
   * @param {number}      [opts.config.ttlMs]            filter eviction TTL
   */
  constructor({ geofenceEngine = null, roomManager = null, config = {} } = {}) {
    this._geofenceEngine = geofenceEngine;
    this._roomManager = roomManager;

    this._enable = config.enable ?? (process.env.PREDICTOR_ENABLE !== "false");
    this._model = (config.model ?? process.env.PREDICTOR_MODEL ?? "CV").toUpperCase();
    this._processNoise =
      config.processNoise ?? Number(process.env.PREDICTOR_PROCESS_NOISE ?? 0.1);
    this._measurementNoise =
      config.measurementNoise ?? Number(process.env.PREDICTOR_MEASUREMENT_NOISE ?? 5.0);
    this._preAlertHorizonS =
      config.preAlertHorizonS ??
      Number(process.env.PREDICTOR_PRE_ALERT_HORIZON_S ?? 60);
    this._maxHorizonS =
      config.maxHorizonS ?? Number(process.env.PREDICTOR_MAX_HORIZON_S ?? 120);
    this._ttlMs = config.ttlMs ?? DEFAULT_TTL_MS;

    /** @type {Map<string, FilterState>} */
    this._filters = new Map();

    /**
     * Debounce map: Map<clientId, Map<fenceId, lastAlertMs>>
     * @type {Map<string, Map<string, number>>}
     */
    this._preAlertDebounce = new Map();

    /**
     * Anomaly rate-limit: Map<clientId, Map<anomalyType, lastEmitMs>>
     * @type {Map<string, Map<string, number>>}
     */
    this._anomalyTimes = new Map();

    // Background eviction — run every 5 minutes
    this._evictionInterval = setInterval(() => this._evict(), 5 * 60 * 1000);
    // Allow the process to exit even if this interval remains
    if (this._evictionInterval.unref) this._evictionInterval.unref();
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Ingest a location update, run the Kalman predict+update cycle,
   * run anomaly detection, and return any anomaly flags.
   *
   * @param {string} clientId
   * @param {object} location
   * @param {number} location.latitude
   * @param {number} location.longitude
   * @param {number} [location.speed]     m/s (optional)
   * @param {number} [location.heading]   degrees (optional)
   * @param {string|number} [location.timestamp]  ISO string or ms epoch
   * @returns {{ anomalies: string[] }}
   */
  update(clientId, location) {
    if (!this._enable) return { anomalies: [] };

    // Anomaly detection runs against the predicted current state (before the
    // filter absorbs the new measurement) so genuine GPS faults are separable
    // from ordinary vehicle motion.
    const anomalies = this.detectAnomalies(clientId, location);

    const ts = this._parseTimestamp(location.timestamp);
    const lat = location.latitude;
    const lon = location.longitude;

    const existing = this._filters.get(clientId);

    if (!existing) {
      // First fix: initialise filter
      const vx = location.speed != null && location.heading != null
        ? location.speed * Math.sin(location.heading * DEG_TO_RAD)
        : 0;
      const vy = location.speed != null && location.heading != null
        ? location.speed * Math.cos(location.heading * DEG_TO_RAD)
        : 0;
      const fs = initFilter(0, 0, vx, vy, lat, lon, ts, this._measurementNoise);
      this._filters.set(clientId, fs);
      return { anomalies };
    }

    // Re-origin if the vehicle has drifted too far from the ENU origin
    const { x: curX, y: curY } = latLonToEnu(lat, lon, existing.originLat, existing.originLon);
    if (enuDistance(0, 0, curX, curY) > ORIGIN_DRIFT_THRESHOLD_M) {
      // Move the origin to the current filtered position (expressed in the old
      // frame), then translate the state by the same offset. A change of
      // origin is a pure translation, so velocity and covariance are unaffected.
      const oldX = existing.x[0];
      const oldY = existing.x[1];
      const newPos = enuToLatLon(oldX, oldY, existing.originLat, existing.originLon);
      existing.originLat = newPos.lat;
      existing.originLon = newPos.lon;
      existing.x[0] -= oldX;
      existing.x[1] -= oldY;
    }

    const dtS = Math.max((ts - existing.lastTimestamp) / 1000, MIN_DT_S);

    // ---- Predict ----
    const F = buildF(dtS);
    const Q = buildQ(dtS, this._processNoise);
    existing.x = mat4MulVec(F, existing.x);
    existing.P = mat4Add(mat4Mul(mat4Mul(F, existing.P), mat4T(F)), Q);

    // ---- Update ----
    const { x: measX, y: measY } = latLonToEnu(lat, lon, existing.originLat, existing.originLon);
    const z = [measX, measY];
    const hx = mulHx(existing.x);
    const innovation = [z[0] - hx[0], z[1] - hx[1]];
    const r = this._measurementNoise ** 2;
    const S = computeS(existing.P, r);
    const K = computeK(existing.P, S);

    if (K) {
      existing.x = applyKx(existing.x, K, innovation);
      existing.P = josephUpdate(existing.P, K, r);
    }

    existing.lastTimestamp = ts;
    existing.lastActivityMs = Date.now();

    return { anomalies };
  }

  /**
   * Returns predicted positions at each horizon (seconds from now).
   *
   * @param {string}   clientId
   * @param {number[]} horizons  seconds, e.g. [10, 30, 60]
   * @returns {Array<{horizon:number, lat:number, lon:number, speed:number, heading:number, confidenceEllipse:{semiMajorM:number,semiMinorM:number,orientationRad:number}}>}
   */
  getTrajectory(clientId, horizons = [10, 30, 60]) {
    if (!this._enable) return [];
    const fs = this._filters.get(clientId);
    if (!fs) return [];

    return horizons
      .filter((h) => h > 0 && h <= this._maxHorizonS)
      .map((h) => {
        const { x, P } = this._predictAt(fs, h);
        const { lat, lon } = enuToLatLon(x[0], x[1], fs.originLat, fs.originLon);
        const speed = Math.sqrt(x[2] ** 2 + x[3] ** 2);
        const heading = (Math.atan2(x[2], x[3]) * RAD_TO_DEG + 360) % 360;
        const posP = [P[0], P[1], P[4], P[5]]; // 2×2 position sub-matrix
        const { lambda1, lambda2 } = sym2Eigen(posP);
        const orientationRad = sym2Angle(posP);
        return {
          horizon: h,
          lat,
          lon,
          speed,
          heading,
          confidenceEllipse: {
            semiMajorM: Math.sqrt(Math.max(0, lambda1)),
            semiMinorM: Math.sqrt(Math.max(0, lambda2)),
            orientationRad,
          },
        };
      });
  }

  /**
   * Compute ETA to a target location.
   *
   * @param {string} clientId
   * @param {number} targetLat
   * @param {number} targetLon
   * @returns {{ etaMean: number|null, etaStdDev: number|null, reachable: boolean }}
   *   etaMean and etaStdDev are in seconds; null means not reachable within maxHorizon.
   */
  getETA(clientId, targetLat, targetLon) {
    if (!this._enable) return { etaMean: null, etaStdDev: null, reachable: false };
    const fs = this._filters.get(clientId);
    if (!fs) return { etaMean: null, etaStdDev: null, reachable: false };

    const { x: tgtX, y: tgtY } = latLonToEnu(targetLat, targetLon, fs.originLat, fs.originLon);

    // Sample trajectory at fine intervals and find crossing
    const step = 1; // 1-second resolution
    let etaMean = null;
    let etaStdDevAccum = null;

    for (let t = step; t <= this._maxHorizonS; t += step) {
      const { x, P } = this._predictAt(fs, t);
      const dist = enuDistance(x[0], x[1], tgtX, tgtY);
      const posUncert = Math.sqrt(Math.max(0, P[0] + P[5])); // sqrt(trace of 2x2 pos)
      if (dist <= posUncert + 1) {
        // Within 1σ position uncertainty + 1m tolerance
        etaMean = t;
        etaStdDevAccum = posUncert / Math.max(this._currentSpeed(fs), 0.1);
        break;
      }
    }

    if (etaMean === null) {
      // Extrapolate rough ETA from current speed and distance
      const distNow = enuDistance(fs.x[0], fs.x[1], tgtX, tgtY);
      const speed = this._currentSpeed(fs);
      if (speed > 0.5) {
        const roughEta = distNow / speed;
        if (roughEta <= this._maxHorizonS) {
          etaMean = roughEta;
          etaStdDevAccum = roughEta * 0.2; // rough 20% uncertainty
        }
      }
    }

    return {
      etaMean,
      etaStdDev: etaStdDevAccum,
      reachable: etaMean !== null,
    };
  }

  /**
   * Evaluate the predicted trajectory against geofences and emit pre-alerts via
   * the room manager for clients in any room.
   *
   * @param {string} clientId
   * @returns {Array<{fenceId:string,predictedEntryTime:number,predictedEntryPoint:{lat:number,lon:number},confidence:number}>}
   */
  checkPreAlerts(clientId) {
    if (!this._enable) return [];
    if (!this._geofenceEngine) return [];
    const fs = this._filters.get(clientId);
    if (!fs) return [];

    const alerts = [];
    const now = Date.now();
    const debounce = this._preAlertDebounce.get(clientId) ?? new Map();

    const horizonS = Math.min(this._preAlertHorizonS, this._maxHorizonS);
    for (let t = TRAJECTORY_SAMPLE_INTERVAL_S; t <= horizonS; t += TRAJECTORY_SAMPLE_INTERVAL_S) {
      const { x, P } = this._predictAt(fs, t);
      const { lat, lon } = enuToLatLon(x[0], x[1], fs.originLat, fs.originLon);

      let fenceHits;
      try {
        fenceHits = this._geofenceEngine.checkPoint?.(lat, lon) ??
          this._geofenceEngine.check?.(lat, lon) ?? [];
      } catch {
        fenceHits = [];
      }

      for (const fence of fenceHits) {
        const fenceId = fence.id ?? fence.fenceId ?? String(fence);
        const fenceName = fence.name ?? fence.fenceName ?? fenceId;
        const lastAlert = debounce.get(fenceId) ?? 0;
        if (now - lastAlert < PRE_ALERT_DEBOUNCE_MS) continue;

        debounce.set(fenceId, now);
        const posVar = Math.max(0, P[0] + P[5]);
        const confidence = Math.exp(-0.5 * posVar / (this._measurementNoise ** 2));

        const alert = {
          fenceId,
          fenceName,
          predictedEntryTime: now + t * 1000,
          predictedEntryPoint: { lat, lon },
          confidence: Math.max(0, Math.min(1, confidence)),
        };
        alerts.push(alert);

        // Broadcast to all rooms the client is in
        this._broadcastEvent(clientId, "geofence_pre_alert", alert);
      }
    }

    this._preAlertDebounce.set(clientId, debounce);
    return alerts;
  }

  /**
   * Detect measurement and kinematic anomalies for an incoming location.
   * Does NOT modify filter state — the innovation is computed against the
   * filter state propagated to the measurement timestamp, so ordinary motion
   * is not mistaken for a GPS fault.
   *
   * @param {string} clientId
   * @param {object} location
   * @returns {string[]}  e.g. ["gps_anomaly", "kinematic_anomaly"]
   */
  detectAnomalies(clientId, location) {
    if (!this._enable) return [];
    const anomalies = [];
    const fs = this._filters.get(clientId);
    const now = Date.now();

    if (fs) {
      const ts = this._parseTimestamp(location.timestamp);
      const dtS = Math.max((ts - fs.lastTimestamp) / 1000, MIN_DT_S);

      // Propagate the filter forward without mutating it.
      const { x: predX, P: predP } = this._predictAt(fs, dtS);
      const { x: measX, y: measY } = latLonToEnu(
        location.latitude,
        location.longitude,
        fs.originLat,
        fs.originLon,
      );
      const hx = mulHx(predX);
      const innovation = [measX - hx[0], measY - hx[1]];
      const r = this._measurementNoise ** 2;
      const S = computeS(predP, r);
      const innovMag = Math.sqrt(innovation[0] ** 2 + innovation[1] ** 2);
      const Sdiag = Math.sqrt(Math.max(S[0], 0) + Math.max(S[3], 0));
      if (innovMag > ANOMALY_SIGMA * Sdiag && Sdiag > 0) {
        if (this._rateAllow(clientId, "gps_anomaly", now)) {
          anomalies.push("gps_anomaly");
        }
      }

      // Kinematic: impossible acceleration or heading change
      if (location.speed != null) {
        const currentSpeed = this._currentSpeed(fs);
        const dvdt = Math.abs(location.speed - currentSpeed) / dtS;
        if (dvdt > MAX_PHYSICAL_ACCEL_MS2) {
          if (this._rateAllow(clientId, "kinematic_anomaly", now)) {
            anomalies.push("kinematic_anomaly");
          }
        }
      }

      if (location.heading != null) {
        const predictedHeading = (Math.atan2(predX[2], predX[3]) * RAD_TO_DEG + 360) % 360;
        const headingDelta = Math.abs(location.heading - predictedHeading);
        const headingDeltaRad = Math.min(headingDelta, 360 - headingDelta) * DEG_TO_RAD;
        const headingRate = headingDeltaRad / dtS;
        if (headingRate > MAX_PHYSICAL_HEADING_RATE && dtS > 1) {
          if (!anomalies.includes("kinematic_anomaly")) {
            if (this._rateAllow(clientId, "kinematic_anomaly", now)) {
              anomalies.push("kinematic_anomaly");
            }
          }
        }
      }
    }

    return anomalies;
  }

  /**
   * Explicitly remove a client's filter state (call on disconnect).
   * @param {string} clientId
   */
  removeClient(clientId) {
    this._filters.delete(clientId);
    this._preAlertDebounce.delete(clientId);
    this._anomalyTimes.delete(clientId);
  }

  /**
   * Serialise a client's filter state for session resumption.
   * @param {string} clientId
   * @returns {object|null}
   */
  serializeState(clientId) {
    const fs = this._filters.get(clientId);
    if (!fs) return null;
    return {
      x: [...fs.x],
      P: [...fs.P],
      originLat: fs.originLat,
      originLon: fs.originLon,
      lastTimestamp: fs.lastTimestamp,
    };
  }

  /**
   * Restore a client's filter state from a session resumption blob.
   * @param {string} clientId
   * @param {object} state  (as returned by serializeState)
   */
  restoreState(clientId, state) {
    if (!state || !Array.isArray(state.x) || state.x.length !== 4) return;
    this._filters.set(clientId, {
      x: [...state.x],
      P: Array.isArray(state.P) && state.P.length === 16 ? [...state.P] : mat4Identity(),
      originLat: state.originLat,
      originLon: state.originLon,
      lastTimestamp: state.lastTimestamp ?? Date.now(),
      lastActivityMs: Date.now(),
    });
  }

  /**
   * Stop background intervals.
   */
  close() {
    clearInterval(this._evictionInterval);
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  /**
   * Predict filter state dt seconds ahead without mutating it.
   * @param {FilterState} fs
   * @param {number}      dtS
   * @returns {{ x: number[], P: number[] }}
   */
  _predictAt(fs, dtS) {
    const F = buildF(dtS);
    const Q = buildQ(dtS, this._processNoise);
    const x = mat4MulVec(F, fs.x);
    const P = mat4Add(mat4Mul(mat4Mul(F, fs.P), mat4T(F)), Q);
    return { x, P };
  }

  /**
   * Current speed estimate from the filter state.
   * @param {FilterState} fs
   * @returns {number}  m/s
   */
  _currentSpeed(fs) {
    return Math.sqrt(fs.x[2] ** 2 + fs.x[3] ** 2);
  }

  /**
   * Parse a timestamp value to milliseconds since epoch.
   * @param {string|number|undefined} ts
   * @returns {number}
   */
  _parseTimestamp(ts) {
    if (ts == null) return Date.now();
    if (typeof ts === "number") return ts;
    const parsed = Date.parse(ts);
    return Number.isNaN(parsed) ? Date.now() : parsed;
  }

  /**
   * Check and record anomaly rate limit.
   * @param {string} clientId
   * @param {string} anomalyType
   * @param {number} now
   * @returns {boolean}  true if allowed to emit
   */
  _rateAllow(clientId, anomalyType, now) {
    if (!this._anomalyTimes.has(clientId)) this._anomalyTimes.set(clientId, new Map());
    const map = this._anomalyTimes.get(clientId);
    const last = map.get(anomalyType) ?? 0;
    if (now - last < ANOMALY_RATE_LIMIT_MS) return false;
    map.set(anomalyType, now);
    return true;
  }

  /**
   * Broadcast a predictive event to all rooms the client occupies.
   * @param {string} clientId
   * @param {string} type
   * @param {object} payload
   */
  _broadcastEvent(clientId, type, payload) {
    if (!this._roomManager) return;
    try {
      const roomIds = this._roomManager.getClientRooms?.(clientId);
      if (!roomIds) return;
      for (const roomId of roomIds) {
        this._roomManager.broadcast(roomId, { type, payload }, clientId);
      }
    } catch (err) {
      logger.warn("predictor: broadcast error", { clientId, type, error: err.message });
    }
  }

  /**
   * Evict filter states that haven't been updated within the TTL.
   */
  _evict() {
    const now = Date.now();
    for (const [clientId, fs] of this._filters) {
      if (now - fs.lastActivityMs > this._ttlMs) {
        this.removeClient(clientId);
        logger.debug("predictor: evicted stale filter", { clientId });
      }
    }
  }
}
