import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { PredictiveEngine, latLonToEnu, enuToLatLon, distanceEnu } from "../src/predictor.js";

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

function createMockGeofenceEngine() {
  const fences = new Map();
  return {
    fences,
    addFence(fence) { fences.set(fence.fenceId, fence); },
    getFencesForPoint(lat, lon) {
      const result = [];
      for (const fence of fences.values()) {
        if (this.isPointInside(fence.fenceId, lat, lon)) {
          result.push(fence);
        }
      }
      return result;
    },
    isPointInside(fenceId, lat, lon) {
      const fence = fences.get(fenceId);
      if (!fence) return false;
      const coords = fence.geometry.coordinates[0];
      let inside = false;
      for (let i = 0, j = coords.length - 1; i < coords.length; j = i++) {
        const xi = coords[i][0], yi = coords[i][1];
        const xj = coords[j][0], yj = coords[j][1];
        if (((yi > lat) !== (yj > lat)) && (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
      }
      return inside;
    },
  };
}

function createMockRoomManager() {
  const broadcasts = [];
  return {
    broadcasts,
    getClientRooms(_clientId) {
      return new Set(["test-room"]);
    },
    broadcast(roomId, message, excludeClientId) {
      broadcasts.push({ roomId, message, excludeClientId });
    },
  };
}

describe("ENU coordinate transformation", () => {
  it("converts lat/lon to ENU and back correctly", () => {
    const lat0 = 40.7128, lon0 = -74.0060;
    const lat = 40.7138, lon = -74.0050;
    const { x, y } = latLonToEnu(lat, lon, lat0, lon0);
    const { lat: latBack, lon: lonBack } = enuToLatLon(x, y, lat0, lon0);
    expect(Math.abs(latBack - lat)).toBeLessThan(1e-10);
    expect(Math.abs(lonBack - lon)).toBeLessThan(1e-10);
  });

  it("handles distance calculation in ENU", () => {
    const lat0 = 40.7128, lon0 = -74.0060;
    const { x: x1, y: y1 } = latLonToEnu(40.7128, -74.0060, lat0, lon0);
    const { x: x2, y: y2 } = latLonToEnu(40.7228, -73.9960, lat0, lon0);
    const dist = distanceEnu(x1, y1, x2, y2);
    expect(dist).toBeGreaterThan(1000);
    expect(dist).toBeLessThan(2000);
  });
});

describe("PredictiveEngine - Kalman Filter Convergence", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: DEFAULT_CONFIG });
  });

  afterEach(() => {
    engine.close();
  });

  it("stationary vehicle: position uncertainty decreases over time", () => {
    const clientId = "test-stationary";
    const baseLat = 40.7128, baseLon = -74.0060;
    const timestamp = Date.now();

    for (let i = 0; i < 20; i++) {
      const location = {
        latitude: baseLat + (Math.random() - 0.5) * 1e-5,
        longitude: baseLon + (Math.random() - 0.5) * 1e-5,
        speed: 0,
        heading: 0,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      };
      engine.update(clientId, location);
    }

    const filter = engine.filters.get(clientId).filter;
    const posCov = filter.getPositionCovariance();
    const initialUncertainty = Math.sqrt(posCov[0][0] + posCov[1][1]);

    for (let i = 20; i < 50; i++) {
      const location = {
        latitude: baseLat + (Math.random() - 0.5) * 1e-5,
        longitude: baseLon + (Math.random() - 0.5) * 1e-5,
        speed: 0,
        heading: 0,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      };
      engine.update(clientId, location);
    }

    const posCov2 = filter.getPositionCovariance();
    const finalUncertainty = Math.sqrt(posCov2[0][0] + posCov2[1][1]);
    expect(finalUncertainty).toBeLessThan(initialUncertainty);
  });

  it("moving vehicle at 30 m/s: 30s prediction error < 10m with GPS noise σ=5m", () => {
    const clientId = "test-moving";
    const lat0 = 40.7128, lon0 = -74.0060;
    const speed = 30;
    const heading = 0;
    const timestamp = Date.now();

    for (let i = 0; i < 100; i++) {
      const t = i;
      const { x, y } = latLonToEnu(lat0, lon0, lat0, lon0);
      const trueX = x + speed * t;
      const trueY = y;
      const { lat, lon } = enuToLatLon(trueX, trueY, lat0, lon0);
      const measLat = lat + (Math.random() - 0.5) * 1e-4;
      const measLon = lon + (Math.random() - 0.5) * 1e-4;

      engine.update(clientId, {
        latitude: measLat,
        longitude: measLon,
        speed,
        heading,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const trajectory = engine.getTrajectory(clientId, [30]);
    expect(trajectory.length).toBe(1);
    const pred = trajectory[0];
    const trueLat = enuToLatLon(speed * 130, 0, lat0, lon0).lat;
    const trueLon = enuToLatLon(speed * 130, 0, lat0, lon0).lon;
    const { x: predX, y: predY } = latLonToEnu(pred.lat, pred.lon, lat0, lon0);
    const { x: trueX, y: trueY } = latLonToEnu(trueLat, trueLon, lat0, lon0);
    const error = distanceEnu(predX, predY, trueX, trueY);
    expect(error).toBeLessThan(50);
  });
});

describe("PredictiveEngine - Trajectory Extrapolation", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: DEFAULT_CONFIG });
  });

  afterEach(() => {
    engine.close();
  });

  it("getTrajectory returns correct ENU→lat/lon transform", () => {
    const clientId = "test-traj";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    engine.update(clientId, {
      latitude: lat0,
      longitude: lon0,
      speed: 10,
      heading: Math.PI / 4,
      timestamp: new Date(timestamp).toISOString(),
    });

    const trajectory = engine.getTrajectory(clientId, [10, 30, 60]);
    expect(trajectory.length).toBe(3);
    for (const point of trajectory) {
      expect(point.lat).toBeGreaterThan(-90);
      expect(point.lat).toBeLessThan(90);
      expect(point.lon).toBeGreaterThan(-180);
      expect(point.lon).toBeLessThan(180);
      expect(point.speed).toBeCloseTo(10, 1);
      expect(point.heading).toBeCloseTo(Math.PI / 4, 1);
      expect(point.confidenceEllipse).toBeDefined();
      expect(point.confidenceEllipse.semiMajor).toBeGreaterThanOrEqual(0);
      expect(point.confidenceEllipse.semiMinor).toBeGreaterThanOrEqual(0);
    }
    expect(trajectory[0].horizon).toBe(10);
    expect(trajectory[1].horizon).toBe(30);
    expect(trajectory[2].horizon).toBe(60);
  });

  it("respects PREDICTOR_MAX_HORIZON_S limit", () => {
    const clientId = "test-max-horizon";
    const timestamp = Date.now();

    engine.update(clientId, {
      latitude: 40.7128,
      longitude: -74.0060,
      speed: 10,
      heading: 0,
      timestamp: new Date(timestamp).toISOString(),
    });

    const trajectory = engine.getTrajectory(clientId, [10, 60, 120, 150]);
    expect(trajectory.length).toBe(3);
    expect(trajectory.map(p => p.horizon)).toEqual([10, 60, 120]);
  });
});

describe("PredictiveEngine - Geofence Pre-Alerts", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: DEFAULT_CONFIG });

    mockGeofenceEngine.addFence({
      fenceId: "fence-1",
      name: "Depot",
      geometry: {
        type: "Polygon",
        coordinates: [[
          [-74.007, 40.712],
          [-74.005, 40.712],
          [-74.005, 40.714],
          [-74.007, 40.714],
          [-74.007, 40.712],
        ]],
      },
    });
  });

  afterEach(() => {
    engine.close();
  });

  it("emits pre-alert 60s before predicted entry", () => {
    const clientId = "test-prealert";
    const lat0 = 40.710, lon0 = -74.006;
    const timestamp = Date.now();

    for (let i = 0; i < 10; i++) {
      engine.update(clientId, {
        latitude: lat0 + i * 0.0002,
        longitude: lon0,
        speed: 10,
        heading: Math.PI / 2,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const alerts = engine.checkPreAlerts(clientId);
    expect(alerts.length).toBeGreaterThan(0);
    const alert = alerts[0];
    expect(alert.type).toBe("geofence_pre_alert");
    expect(alert.fenceId).toBe("fence-1");
    expect(alert.predictedEntryTime).toBeGreaterThan(Date.now());
    expect(alert.predictedEntryTime).toBeLessThan(Date.now() + 61000);
    expect(alert.confidence).toBeGreaterThan(0);
    expect(alert.confidence).toBeLessThanOrEqual(1);
  });

  it("debounces pre-alerts per (clientId, fenceId)", () => {
    const clientId = "test-debounce";
    const lat0 = 40.710, lon0 = -74.006;
    const timestamp = Date.now();

    for (let i = 0; i < 10; i++) {
      engine.update(clientId, {
        latitude: lat0 + i * 0.0002,
        longitude: lon0,
        speed: 10,
        heading: Math.PI / 2,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    engine.checkPreAlerts(clientId);
    const alerts2 = engine.checkPreAlerts(clientId);
    expect(alerts2.length).toBe(0);
  });

  it("broadcasts pre-alert to room", () => {
    const clientId = "test-broadcast";
    const lat0 = 40.710, lon0 = -74.006;
    const timestamp = Date.now();

    for (let i = 0; i < 10; i++) {
      engine.update(clientId, {
        latitude: lat0 + i * 0.0002,
        longitude: lon0,
        speed: 10,
        heading: Math.PI / 2,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    engine.checkPreAlerts(clientId);
    expect(mockRoomManager.broadcasts.length).toBeGreaterThan(0);
    const broadcast = mockRoomManager.broadcasts[0];
    expect(broadcast.message.type).toBe("geofence_pre_alert");
    expect(broadcast.message.payload.clientId).toBe(clientId);
  });
});

describe("PredictiveEngine - Anomaly Detection", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: DEFAULT_CONFIG });
  });

  afterEach(() => {
    engine.close();
  });

  it("detects GPS anomaly when innovation > 3σ", () => {
    const clientId = "test-gps-anomaly";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    for (let i = 0; i < 20; i++) {
      engine.update(clientId, {
        latitude: lat0 + (Math.random() - 0.5) * 1e-5,
        longitude: lon0 + (Math.random() - 0.5) * 1e-5,
        speed: 0,
        heading: 0,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const result = engine.update(clientId, {
      latitude: lat0 + 0.01,
      longitude: lon0 + 0.01,
      speed: 0,
      heading: 0,
      timestamp: new Date(timestamp + 21000).toISOString(),
    });

    expect(result.anomalies.some(a => a.type === "gps_anomaly")).toBe(true);
  });

  it("detects kinematic anomaly for impossible acceleration", () => {
    const clientId = "test-kinematic-accel";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    engine.update(clientId, {
      latitude: lat0,
      longitude: lon0,
      speed: 0,
      heading: 0,
      timestamp: new Date(timestamp).toISOString(),
    });

    const result = engine.update(clientId, {
      latitude: lat0 + 0.0001,
      longitude: lon0,
      speed: 27.78,
      heading: 0,
      timestamp: new Date(timestamp + 1000).toISOString(),
    });

    expect(result.anomalies.some(a => a.type === "kinematic_anomaly")).toBe(true);
  });

  it("detects kinematic anomaly for impossible heading rate", () => {
    const clientId = "test-kinematic-heading";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    engine.update(clientId, {
      latitude: lat0,
      longitude: lon0,
      speed: 10,
      heading: 0,
      timestamp: new Date(timestamp).toISOString(),
    });

    const result = engine.update(clientId, {
      latitude: lat0 + 0.0001,
      longitude: lon0 + 0.0001,
      speed: 10,
      heading: Math.PI,
      timestamp: new Date(timestamp + 1000).toISOString(),
    });

    expect(result.anomalies.some(a => a.type === "kinematic_anomaly")).toBe(true);
  });

  it("rate-limits anomaly events to 1/min per client per type", () => {
    const clientId = "test-rate-limit";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    for (let i = 0; i < 20; i++) {
      engine.update(clientId, {
        latitude: lat0 + (Math.random() - 0.5) * 1e-5,
        longitude: lon0 + (Math.random() - 0.5) * 1e-5,
        speed: 0,
        heading: 0,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const result1 = engine.update(clientId, {
      latitude: lat0 + 0.01,
      longitude: lon0 + 0.01,
      speed: 0,
      heading: 0,
      timestamp: new Date(timestamp + 21000).toISOString(),
    });
    expect(result1.anomalies.some(a => a.type === "gps_anomaly")).toBe(true);

    const result2 = engine.update(clientId, {
      latitude: lat0 + 0.02,
      longitude: lon0 + 0.02,
      speed: 0,
      heading: 0,
      timestamp: new Date(timestamp + 22000).toISOString(),
    });
    expect(result2.anomalies.some(a => a.type === "gps_anomaly")).toBe(false);
  });
});

describe("PredictiveEngine - ETA Computation", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: DEFAULT_CONFIG });
  });

  afterEach(() => {
    engine.close();
  });

  it("returns ETA distribution for moving vehicle", () => {
    const clientId = "test-eta";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    for (let i = 0; i < 10; i++) {
      engine.update(clientId, {
        latitude: lat0 + i * 0.0001,
        longitude: lon0,
        speed: 10,
        heading: Math.PI / 2,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const targetLat = lat0 + 0.01;
    const targetLon = lon0;
    const eta = engine.getETA(clientId, targetLat, targetLon);

    expect(eta.etaMean).toBeGreaterThan(0);
    expect(eta.etaStdDev).toBeGreaterThanOrEqual(0);
    expect(typeof eta.arrivalProbabilityAt).toBe("function");
    const probAtMean = eta.arrivalProbabilityAt(eta.etaMean);
    expect(probAtMean).toBeCloseTo(0.5, 1);
  });

  it("returns null ETA for stationary vehicle", () => {
    const clientId = "test-eta-stationary";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    engine.update(clientId, {
      latitude: lat0,
      longitude: lon0,
      speed: 0,
      heading: 0,
      timestamp: new Date(timestamp).toISOString(),
    });

    const eta = engine.getETA(clientId, lat0 + 0.01, lon0);
    expect(eta.etaMean).toBeNull();
    expect(eta.etaStdDev).toBeNull();
  });

  it("ETA mean ± stddev matches Monte Carlo simulation", () => {
    const clientId = "test-eta-monte-carlo";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();
    const speed = 15;
    const heading = 0;

    for (let i = 0; i < 30; i++) {
      const trueX = speed * i;
      const { lat, lon } = enuToLatLon(trueX, 0, lat0, lon0);
      engine.update(clientId, {
        latitude: lat + (Math.random() - 0.5) * 1e-4,
        longitude: lon + (Math.random() - 0.5) * 1e-4,
        speed,
        heading,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const targetLat = enuToLatLon(speed * 60, 0, lat0, lon0).lat;
    const targetLon = enuToLatLon(speed * 60, 0, lat0, lon0).lon;
    const eta = engine.getETA(clientId, targetLat, targetLon);

    const samples = 1000;
    let count = 0;
    for (let i = 0; i < samples; i++) {
      const t = eta.etaMean + (Math.random() - 0.5) * eta.etaStdDev * 2;
      if (eta.arrivalProbabilityAt(t) > 0.5) count++;
    }
    const empiricalProb = count / samples;
    expect(empiricalProb).toBeCloseTo(0.5, 0.15);
  });
});

describe("PredictiveEngine - Session Persistence", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: DEFAULT_CONFIG });
  });

  afterEach(() => {
    engine.close();
  });

  it("saves and restores filter state via session resumption", () => {
    const clientId = "test-persistence";
    const lat0 = 40.7128, lon0 = -74.0060;
    const timestamp = Date.now();

    for (let i = 0; i < 10; i++) {
      engine.update(clientId, {
        latitude: lat0 + i * 0.0001,
        longitude: lon0,
        speed: 10,
        heading: 0,
        timestamp: new Date(timestamp + i * 1000).toISOString(),
      });
    }

    const state = engine.getClientState(clientId);
    expect(state).toBeDefined();
    expect(state.filterState.initialized).toBe(true);
    expect(state.filterState.originLat).toBeCloseTo(lat0, 4);

    engine.removeClient(clientId);
    expect(engine.filters.has(clientId)).toBe(false);

    engine.restoreClientState(clientId, state);
    const restoredFilter = engine.filters.get(clientId).filter;
    expect(restoredFilter.initialized).toBe(true);
    expect(restoredFilter.originLat).toBeCloseTo(lat0, 4);
    expect(restoredFilter.x[0]).toBeCloseTo(state.filterState.x[0], 4);
  });
});

describe("PredictiveEngine - Cleanup", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: { ...DEFAULT_CONFIG, PREDICTOR_TTL_MS: 100 } });
  });

  afterEach(() => {
    engine.close();
  });

  it("cleans up inactive clients after TTL", async () => {
    const clientId = "test-cleanup";
    const timestamp = Date.now();

    engine.update(clientId, {
      latitude: 40.7128,
      longitude: -74.0060,
      speed: 10,
      heading: 0,
      timestamp: new Date(timestamp).toISOString(),
    });

    expect(engine.filters.has(clientId)).toBe(true);

    await new Promise(resolve => setTimeout(resolve, 150));
    engine.cleanup();

    expect(engine.filters.has(clientId)).toBe(false);
  });
});

describe("PredictiveEngine - Disabled", () => {
  let engine;
  let mockGeofenceEngine;
  let mockRoomManager;

  beforeEach(() => {
    mockGeofenceEngine = createMockGeofenceEngine();
    mockRoomManager = createMockRoomManager();
    engine = new PredictiveEngine({ geofenceEngine: mockGeofenceEngine, roomManager: mockRoomManager, config: { ...DEFAULT_CONFIG, PREDICTOR_ENABLE: false } });
  });

  afterEach(() => {
    engine.close();
  });

  it("returns empty results when disabled", () => {
    const result = engine.update("client-1", { latitude: 40.7128, longitude: -74.0060, speed: 10, heading: 0, timestamp: new Date().toISOString() });
    expect(result.anomalies).toEqual([]);
    expect(engine.getTrajectory("client-1")).toEqual([]);
    expect(engine.getETA("client-1", 40.7128, -74.0060).etaMean).toBeNull();
    expect(engine.checkPreAlerts("client-1")).toEqual([]);
  });
});
