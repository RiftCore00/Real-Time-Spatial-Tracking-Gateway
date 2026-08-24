

const EARTH_RADIUS = 6371000;
const DEFAULT_CONFIG = {
  PREDICTOR_ENABLE: true,
  PREDICTOR_MODEL: "CV",
  PREDICTOR_PROCESS_NOISE: 0.1,
  PREDICTOR_MEASUREMENT_NOISE: 5.0,
  PREDICTOR_PRE_ALERT_HORIZON_S: 60,
  PREDICTOR_MAX_HORIZON_S: 120,
  PREDICTOR_RE_ORIGIN_DISTANCE_KM: 50,
  PREDICTOR_ANOMALY_INNOVATION_SIGMA: 3,
  PREDICTOR_ANOMALY_MAX_ACCELERATION: 10,
  PREDICTOR_ANOMALY_MAX_HEADING_RATE: Math.PI / 2,
  PREDICTOR_ANOMALY_RATE_LIMIT_MS: 60000,
  PREDICTOR_TTL_MS: 3600000,
  PREDICTOR_ETA_SAMPLES: 1000,
};

function parseConfig(env = process.env) {
  const config = { ...DEFAULT_CONFIG };
  for (const key of Object.keys(DEFAULT_CONFIG)) {
    if (env[key] !== undefined) {
      const val = env[key];
      const def = DEFAULT_CONFIG[key];
      if (typeof def === "boolean") {
        config[key] = val === "true";
      } else if (typeof def === "number") {
        config[key] = Number(val);
      } else {
        config[key] = val;
      }
    }
  }
  return config;
}

function latLonToEnu(lat, lon, lat0, lon0) {
  const dLat = (lat - lat0) * Math.PI / 180;
  const dLon = (lon - lon0) * Math.PI / 180;
  const x = EARTH_RADIUS * dLon * Math.cos(lat0 * Math.PI / 180);
  const y = EARTH_RADIUS * dLat;
  return { x, y };
}

function enuToLatLon(x, y, lat0, lon0) {
  const lat = lat0 + y / EARTH_RADIUS * 180 / Math.PI;
  const lon = lon0 + x / (EARTH_RADIUS * Math.cos(lat0 * Math.PI / 180)) * 180 / Math.PI;
  return { lat, lon };
}

function distanceEnu(x1, y1, x2, y2) {
  const dx = x2 - x1;
  const dy = y2 - y1;
  return Math.sqrt(dx * dx + dy * dy);
}

class KalmanFilterCV {
  constructor(processNoise, measurementNoise) {
    this.x = [0, 0, 0, 0];
    this.P = [
      [100, 0, 0, 0],
      [0, 100, 0, 0],
      [0, 0, 100, 0],
      [0, 0, 0, 100],
    ];
    this.Q = this.createProcessNoise(processNoise);
    this.R = [
      [measurementNoise * measurementNoise, 0],
      [0, measurementNoise * measurementNoise],
    ];
    this.initialized = false;
    this.lastTimestamp = null;
    this.originLat = null;
    this.originLon = null;
  }

  createProcessNoise(q) {
    return [
      [q, 0, 0, 0],
      [0, q, 0, 0],
      [0, 0, q, 0],
      [0, 0, 0, q],
    ];
  }

  initialize(lat, lon, timestamp) {
    this.originLat = lat;
    this.originLon = lon;
    this.lastTimestamp = timestamp;
    this.initialized = true;
  }

  predict(dt) {
    const F = [
      [1, 0, dt, 0],
      [0, 1, 0, dt],
      [0, 0, 1, 0],
      [0, 0, 0, 1],
    ];
    this.x = this.matVecMul(F, this.x);
    this.P = this.matAdd(this.matMul(this.matMul(F, this.P), this.transpose(F)), this.Q);
  }

  update(z, timestamp) {
    if (!this.initialized) return { innovation: [0, 0], innovationCov: [[0, 0], [0, 0]] };

    if (this.lastTimestamp !== null) {
      const dt = (timestamp - this.lastTimestamp) / 1000;
      if (dt > 0) this.predict(dt);
    }
    this.lastTimestamp = timestamp;

    const H = [
      [1, 0, 0, 0],
      [0, 1, 0, 0],
    ];

    const Hx = [this.x[0], this.x[1]];
    const y = [z[0] - Hx[0], z[1] - Hx[1]];

    const HPHt = this.matMul(this.matMul(H, this.P), this.transpose(H));
    const S = this.matAdd(HPHt, this.R);

    const Sinv = this.inv2x2(S);
    const PHt = this.matMul(this.P, this.transpose(H));
    const K = this.matMul(PHt, Sinv);

    const Ky = this.matVecMul(K, y);
    this.x = [this.x[0] + Ky[0], this.x[1] + Ky[1], this.x[2] + Ky[2], this.x[3] + Ky[3]];

    const I = [
      [1, 0, 0, 0],
      [0, 1, 0, 0],
      [0, 0, 1, 0],
      [0, 0, 0, 1],
    ];
    const KH = this.matMul(K, H);
    const IminusKH = this.matSub(I, KH);
    const IminusKHP = this.matMul(IminusKH, this.P);
    const IminusKHP_IminusKHt = this.matMul(IminusKHP, this.transpose(IminusKH));
    const KRKt = this.matMul(this.matMul(K, this.R), this.transpose(K));
    this.P = this.matAdd(IminusKHP_IminusKHt, KRKt);

    return { innovation: y, innovationCov: S };
  }

  predictState(dt) {
    const F = [
      [1, 0, dt, 0],
      [0, 1, 0, dt],
      [0, 0, 1, 0],
      [0, 0, 0, 1],
    ];
    const xPred = this.matVecMul(F, this.x);
    const PPred = this.matAdd(this.matMul(this.matMul(F, this.P), this.transpose(F)), this.Q);
    return { x: xPred, P: PPred };
  }

  getState() {
    return {
      x: this.x[0],
      y: this.x[1],
      vx: this.x[2],
      vy: this.x[3],
      speed: Math.sqrt(this.x[2] * this.x[2] + this.x[3] * this.x[3]),
      heading: Math.atan2(this.x[3], this.x[2]),
      covariance: this.P,
    };
  }

  getPositionCovariance() {
    return [
      [this.P[0][0], this.P[0][1]],
      [this.P[1][0], this.P[1][1]],
    ];
  }

  setState(x, P) {
    this.x = x;
    this.P = P;
  }

  matMul(A, B) {
    const rowsA = A.length;
    const colsA = A[0].length;
    const colsB = B[0].length;
    const result = Array(rowsA).fill(null).map(() => Array(colsB).fill(0));
    for (let i = 0; i < rowsA; i++) {
      for (let j = 0; j < colsB; j++) {
        let sum = 0;
        for (let k = 0; k < colsA; k++) {
          sum += A[i][k] * B[k][j];
        }
        result[i][j] = sum;
      }
    }
    return result;
  }

  matVecMul(A, v) {
    const rows = A.length;
    const cols = A[0].length;
    const result = Array(rows).fill(0);
    for (let i = 0; i < rows; i++) {
      let sum = 0;
      for (let j = 0; j < cols; j++) {
        sum += A[i][j] * v[j];
      }
      result[i] = sum;
    }
    return result;
  }

  matAdd(A, B) {
    const rows = A.length;
    const cols = A[0].length;
    const result = Array(rows).fill(null).map(() => Array(cols).fill(0));
    for (let i = 0; i < rows; i++) {
      for (let j = 0; j < cols; j++) {
        result[i][j] = A[i][j] + B[i][j];
      }
    }
    return result;
  }

  matSub(A, B) {
    const rows = A.length;
    const cols = A[0].length;
    const result = Array(rows).fill(null).map(() => Array(cols).fill(0));
    for (let i = 0; i < rows; i++) {
      for (let j = 0; j < cols; j++) {
        result[i][j] = A[i][j] - B[i][j];
      }
    }
    return result;
  }

  transpose(A) {
    const rows = A.length;
    const cols = A[0].length;
    const result = Array(cols).fill(null).map(() => Array(rows).fill(0));
    for (let i = 0; i < rows; i++) {
      for (let j = 0; j < cols; j++) {
        result[j][i] = A[i][j];
      }
    }
    return result;
  }

  inv2x2(M) {
    const a = M[0][0], b = M[0][1], c = M[1][0], d = M[1][1];
    const det = a * d - b * c;
    if (Math.abs(det) < 1e-12) {
      return [[1e12, 0], [0, 1e12]];
    }
    return [[d / det, -b / det], [-c / det, a / det]];
  }

  eigenvalues2x2(M) {
    const a = M[0][0], b = M[0][1], c = M[1][0], d = M[1][1];
    const trace = a + d;
    const det = a * d - b * c;
    const discriminant = trace * trace - 4 * det;
    if (discriminant < 0) return [trace / 2, trace / 2];
    const sqrtDisc = Math.sqrt(discriminant);
    const lambda1 = (trace + sqrtDisc) / 2;
    const lambda2 = (trace - sqrtDisc) / 2;
    return [lambda1, lambda2];
  }

  confidenceEllipse() {
    const Ppos = this.getPositionCovariance();
    const eig = this.eigenvalues2x2(Ppos);
    const a = Math.sqrt(Math.max(0, eig[0]));
    const b = Math.sqrt(Math.max(0, eig[1]));
    const angle = 0.5 * Math.atan2(2 * Ppos[0][1], Ppos[0][0] - Ppos[1][1]);
    return { semiMajor: a, semiMinor: b, orientation: angle };
  }

  shouldReorigin(lat, lon) {
    if (this.originLat === null || this.originLon === null) return false;
    const { x, y } = latLonToEnu(lat, lon, this.originLat, this.originLon);
    const dist = Math.sqrt(x * x + y * y);
    return dist > 50000;
  }

  reorigin(lat, lon) {
    const { x, y } = latLonToEnu(lat, lon, this.originLat, this.originLon);
    this.x[0] = x;
    this.x[1] = y;
    this.originLat = lat;
    this.originLon = lon;
  }
}

class ClientFilter {
  constructor(config) {
    this.config = config;
    this.filter = new KalmanFilterCV(config.PREDICTOR_PROCESS_NOISE, config.PREDICTOR_MEASUREMENT_NOISE);
    this.lastLocation = null;
    this.lastUpdateTime = null;
    this.preAlertDebounce = new Map();
    this.anomalyRateLimit = new Map();
    this.predictedPositions = [];
  }

  update(location) {
    const { latitude, longitude, speed, heading, timestamp } = location;
    const ts = timestamp ? Date.parse(timestamp) : Date.now();

    if (!this.filter.initialized) {
      this.filter.initialize(latitude, longitude, ts);
      const enu = latLonToEnu(latitude, longitude, this.filter.originLat, this.filter.originLon);
      this.filter.x[0] = enu.x;
      this.filter.x[1] = enu.y;
      if (speed !== undefined && heading !== undefined) {
        this.filter.x[2] = speed * Math.cos(heading);
        this.filter.x[3] = speed * Math.sin(heading);
      }
      this.lastLocation = { latitude, longitude, speed, heading, timestamp: ts };
      this.lastUpdateTime = ts;
      return { innovation: [0, 0], innovationCov: [[0, 0], [0, 0]], anomalies: [] };
    }

    if (this.filter.shouldReorigin(latitude, longitude)) {
      this.filter.reorigin(latitude, longitude);
    }

    const enu = latLonToEnu(latitude, longitude, this.filter.originLat, this.filter.originLon);
    const result = this.filter.update([enu.x, enu.y], ts);

    const anomalies = this.detectAnomalies(location, result, ts);

    this.lastLocation = { latitude, longitude, speed, heading, timestamp: ts };
    this.lastUpdateTime = ts;

    return { ...result, anomalies };
  }

  detectAnomalies(location, filterResult, timestamp) {
    const anomalies = [];
    const now = Date.now();
    const clientId = this.clientId;

    const { innovation, innovationCov } = filterResult;
    const sigmaX = Math.sqrt(innovationCov[0][0]);
    const sigmaY = Math.sqrt(innovationCov[1][1]);
    const innovationNorm = Math.sqrt(innovation[0] * innovation[0] + innovation[1] * innovation[1]);
    const sigmaNorm = Math.sqrt(sigmaX * sigmaX + sigmaY * sigmaY);

    if (innovationNorm > this.config.PREDICTOR_ANOMALY_INNOVATION_SIGMA * sigmaNorm) {
      if (this.canEmitAnomaly(clientId, "gps_anomaly", now)) {
        anomalies.push({ type: "gps_anomaly", severity: "warning", innovation: innovationNorm, threshold: this.config.PREDICTOR_ANOMALY_INNOVATION_SIGMA * sigmaNorm });
      }
    }

    if (this.lastLocation && location.speed !== undefined && this.lastLocation.speed !== undefined) {
      const dt = (timestamp - this.lastUpdateTime) / 1000;
      if (dt > 0) {
        const accel = Math.abs(location.speed - this.lastLocation.speed) / dt;
        if (accel > this.config.PREDICTOR_ANOMALY_MAX_ACCELERATION) {
          if (this.canEmitAnomaly(clientId, "kinematic_anomaly", now)) {
            anomalies.push({ type: "kinematic_anomaly", severity: "warning", acceleration: accel, threshold: this.config.PREDICTOR_ANOMALY_MAX_ACCELERATION });
          }
        }
      }
    }

    if (this.lastLocation && location.heading !== undefined && this.lastLocation.heading !== undefined) {
      const dt = (timestamp - this.lastUpdateTime) / 1000;
      if (dt > 0) {
        let headingDiff = location.heading - this.lastLocation.heading;
        headingDiff = ((headingDiff + Math.PI) % (2 * Math.PI)) - Math.PI;
        const headingRate = Math.abs(headingDiff) / dt;
        if (headingRate > this.config.PREDICTOR_ANOMALY_MAX_HEADING_RATE) {
          if (this.canEmitAnomaly(clientId, "kinematic_anomaly", now)) {
            anomalies.push({ type: "kinematic_anomaly", severity: "warning", headingRate, threshold: this.config.PREDICTOR_ANOMALY_MAX_HEADING_RATE });
          }
        }
      }
    }

    return anomalies;
  }

  canEmitAnomaly(clientId, type, now) {
    const key = `${clientId}:${type}`;
    const lastEmit = this.anomalyRateLimit.get(key) || 0;
    if (now - lastEmit >= this.config.PREDICTOR_ANOMALY_RATE_LIMIT_MS) {
      this.anomalyRateLimit.set(key, now);
      return true;
    }
    return false;
  }

  getTrajectory(horizons) {
    const results = [];
    const { originLat, originLon } = this.filter;

    for (const horizon of horizons) {
      if (horizon > this.config.PREDICTOR_MAX_HORIZON_S) continue;
      const pred = this.filter.predictState(horizon);
      const { x, y } = { x: pred.x[0], y: pred.x[1] };
      const { lat, lon } = enuToLatLon(x, y, originLat, originLon);
      const speed = Math.sqrt(pred.x[2] * pred.x[2] + pred.x[3] * pred.x[3]);
      const heading = Math.atan2(pred.x[3], pred.x[2]);
      const posCov = [
        [pred.P[0][0], pred.P[0][1]],
        [pred.P[1][0], pred.P[1][1]],
      ];
      const eig = this.filter.eigenvalues2x2(posCov);
      const semiMajor = Math.sqrt(Math.max(0, eig[0]));
      const semiMinor = Math.sqrt(Math.max(0, eig[1]));
      const orientation = 0.5 * Math.atan2(2 * posCov[0][1], posCov[0][0] - posCov[1][1]);
      results.push({
        horizon,
        lat,
        lon,
        speed,
        heading,
        confidenceEllipse: { semiMajor, semiMinor, orientation },
      });
    }
    return results;
  }

  getETA(targetLat, targetLon) {
    const state = this.filter.getState();
    const { originLat, originLon } = this.filter;
    const targetEnu = latLonToEnu(targetLat, targetLon, originLat, originLon);
    const dx = targetEnu.x - state.x;
    const dy = targetEnu.y - state.y;
    const distance = Math.sqrt(dx * dx + dy * dy);
    const speed = state.speed;
    if (speed < 0.1) {
      return { etaMean: null, etaStdDev: null, arrivalProbabilityAt: () => 0 };
    }
    const etaMean = distance / speed;
    const posCov = this.filter.getPositionCovariance();
    const alongTrackVar = (dx * dx * posCov[0][0] + 2 * dx * dy * posCov[0][1] + dy * dy * posCov[1][1]) / (distance * distance);
    const etaStdDev = Math.sqrt(alongTrackVar) / speed;
    return {
      etaMean,
      etaStdDev,
      arrivalProbabilityAt: (t) => {
        const z = (t - etaMean) / etaStdDev;
        return 0.5 * (1 + this.erf(z / Math.sqrt(2)));
      },
    };
  }

  erf(x) {
    const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741, a4 = -1.453152027, a5 = 1.061405429;
    const p = 0.3275911;
    const sign = x < 0 ? -1 : 1;
    x = Math.abs(x);
    const t = 1 / (1 + p * x);
    const y = 1 - ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
    return sign * y;
  }

  checkPreAlerts(geofenceEngine) {
    if (!geofenceEngine) return [];
    const alerts = [];
    const horizons = [10, 20, 30, 40, 50, 60].filter(h => h <= this.config.PREDICTOR_PRE_ALERT_HORIZON_S);

    for (const horizon of horizons) {
      const pred = this.filter.predictState(horizon);
      const { x, y } = { x: pred.x[0], y: pred.x[1] };
      const { lat, lon } = enuToLatLon(x, y, this.filter.originLat, this.filter.originLon);
      const predictedTime = Date.now() + horizon * 1000;

      const fences = geofenceEngine.getFencesForPoint?.(lat, lon) || [];
      for (const fence of fences) {
        const key = `${this.clientId}:${fence.fenceId}`;
        if (this.preAlertDebounce.has(key)) continue;
        const inside = geofenceEngine.isPointInside?.(fence.fenceId, lat, lon);
        if (inside) {
          this.preAlertDebounce.set(key, true);
          alerts.push({
            type: "geofence_pre_alert",
            fenceId: fence.fenceId,
            fenceName: fence.name,
            predictedEntryTime: predictedTime,
            predictedEntryPoint: { lat, lon },
            confidence: 1 - Math.min(1, horizon / this.config.PREDICTOR_PRE_ALERT_HORIZON_S),
          });
        }
      }
    }
    return alerts;
  }

  getStateForPersistence() {
    return {
      filterState: {
        x: this.filter.x,
        P: this.filter.P,
        initialized: this.filter.initialized,
        originLat: this.filter.originLat,
        originLon: this.filter.originLon,
        lastTimestamp: this.filter.lastTimestamp,
      },
      lastLocation: this.lastLocation,
      lastUpdateTime: this.lastUpdateTime,
    };
  }

  restoreState(state) {
    if (!state || !state.filterState) return;
    const { filterState } = state;
    this.filter.x = filterState.x;
    this.filter.P = filterState.P;
    this.filter.initialized = filterState.initialized;
    this.filter.originLat = filterState.originLat;
    this.filter.originLon = filterState.originLon;
    this.filter.lastTimestamp = filterState.lastTimestamp;
    this.lastLocation = state.lastLocation;
    this.lastUpdateTime = state.lastUpdateTime;
  }
}

export class PredictiveEngine {
  constructor({ geofenceEngine, roomManager, config: userConfig = {} }) {
    this.geofenceEngine = geofenceEngine;
    this.roomManager = roomManager;
    this.config = { ...DEFAULT_CONFIG, ...parseConfig(), ...userConfig };
    this.filters = new Map();
    this.cleanupInterval = setInterval(() => this.cleanup(), 300000);
    this.cleanupInterval.unref();
  }

  update(clientId, location) {
    if (!this.config.PREDICTOR_ENABLE) return { anomalies: [] };

    let clientFilter = this.filters.get(clientId);
    if (!clientFilter) {
      clientFilter = new ClientFilter(this.config);
      clientFilter.clientId = clientId;
      this.filters.set(clientId, clientFilter);
    }
    return clientFilter.update(location);
  }

  getTrajectory(clientId, horizons = [10, 30, 60]) {
    const clientFilter = this.filters.get(clientId);
    if (!clientFilter || !clientFilter.filter.initialized) return [];
    return clientFilter.getTrajectory(horizons);
  }

  getETA(clientId, targetLat, targetLon) {
    const clientFilter = this.filters.get(clientId);
    if (!clientFilter || !clientFilter.filter.initialized) {
      return { etaMean: null, etaStdDev: null, arrivalProbabilityAt: () => 0 };
    }
    return clientFilter.getETA(targetLat, targetLon);
  }

  checkPreAlerts(clientId) {
    const clientFilter = this.filters.get(clientId);
    if (!clientFilter || !clientFilter.filter.initialized) return [];
    const alerts = clientFilter.checkPreAlerts(this.geofenceEngine);
    for (const alert of alerts) {
      this.broadcastPreAlert(clientId, alert);
    }
    return alerts;
  }

  detectAnomalies(clientId, location) {
    const clientFilter = this.filters.get(clientId);
    if (!clientFilter) return [];
    return clientFilter.detectAnomalies(location, { innovation: [0, 0], innovationCov: [[0, 0], [0, 0]] }, Date.now());
  }

  broadcastPreAlert(clientId, alert) {
    if (!this.roomManager) return;
    const rooms = this.roomManager.getClientRooms(clientId);
    for (const roomId of rooms) {
      this.roomManager.broadcast(roomId, {
        type: "geofence_pre_alert",
        payload: { clientId, ...alert },
      }, clientId);
    }
  }

  broadcastAnomaly(clientId, anomaly) {
    if (!this.roomManager) return;
    const rooms = this.roomManager.getClientRooms(clientId);
    for (const roomId of rooms) {
      this.roomManager.broadcast(roomId, {
        type: "gps_anomaly",
        payload: { clientId, ...anomaly },
      }, clientId);
    }
  }

  broadcastETAUpdate(clientId, eta) {
    if (!this.roomManager) return;
    const rooms = this.roomManager.getClientRooms(clientId);
    for (const roomId of rooms) {
      this.roomManager.broadcast(roomId, {
        type: "eta_update",
        payload: { clientId, ...eta },
      }, clientId);
    }
  }

  getClientState(clientId) {
    const clientFilter = this.filters.get(clientId);
    if (!clientFilter) return null;
    return clientFilter.getStateForPersistence();
  }

  restoreClientState(clientId, state) {
    let clientFilter = this.filters.get(clientId);
    if (!clientFilter) {
      clientFilter = new ClientFilter(this.config);
      clientFilter.clientId = clientId;
      this.filters.set(clientId, clientFilter);
    }
    clientFilter.restoreState(state);
  }

  removeClient(clientId) {
    this.filters.delete(clientId);
  }

  cleanup() {
    const now = Date.now();
    for (const [clientId, clientFilter] of this.filters) {
      if (clientFilter.lastUpdateTime && now - clientFilter.lastUpdateTime > this.config.PREDICTOR_TTL_MS) {
        this.filters.delete(clientId);
      }
    }
  }

  close() {
    clearInterval(this.cleanupInterval);
    this.filters.clear();
  }
}

export { latLonToEnu, enuToLatLon, distanceEnu };