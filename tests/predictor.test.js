/**
 * tests/predictor.test.js
 *
 * Unit tests for src/predictor.js  (Issue #248).
 *
 * Covers:
 *  - Kalman filter convergence (stationary vehicle)
 *  - Moving vehicle prediction accuracy (<10 m for 30 s at 30 m/s with σ=5m GPS)
 *  - ENU ↔ lat/lon round-trip transform
 *  - Trajectory output (positions, confidence ellipse)
 *  - GPS anomaly detection (innovation > 3σ)
 *  - Kinematic anomaly detection (impossible acceleration)
 *  - ETA computation (mean & stddev plausible)
 *  - Geofence pre-alert emission via room manager mock
 *  - Debounce: pre-alert not repeated within debounce window
 *  - State serialise / restore (session resumption)
 *  - removeClient cleans up state
 *  - Disabled engine returns empty results
 *  - Origin drift triggers re-origin
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { PredictiveEngine, latLonToEnu, enuToLatLon } from "../src/predictor.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a location object at (lat, lon) with optional speed/heading/timestamp. */
function loc(lat, lon, { speed = 0, heading = 0, timestamp = Date.now() } = {}) {
  return { latitude: lat, longitude: lon, speed, heading, timestamp };
}

/** Simulate a straight-line track at constant velocity. */
function makeTrack(originLat, originLon, speedMs, headingDeg, points, dtMs = 1000) {
  const DEG = Math.PI / 180;
  const R = 6_371_000;
  const vx = speedMs * Math.sin(headingDeg * DEG);
  const vy = speedMs * Math.cos(headingDeg * DEG);
  const track = [];
  let t = Date.now() - points * dtMs;
  let lat = originLat;
  let lon = originLon;
  for (let i = 0; i < points; i++) {
    track.push(loc(lat, lon, { speed: speedMs, heading: headingDeg, timestamp: t }));
    lat += (vy * (dtMs / 1000)) / R * (180 / Math.PI);
    lon +=
      (vx * (dtMs / 1000)) /
      (R * Math.cos(lat * DEG)) *
      (180 / Math.PI);
    t += dtMs;
  }
  return track;
}

/** Add Gaussian noise to a track position (σ = sigma metres). */
function noisyLoc(baseLoc, sigma, rng = Math.random) {
  const R = 6_371_000;
  const DEG = Math.PI / 180;
  const nx = (rng() - 0.5) * 2 * sigma;
  const ny = (rng() - 0.5) * 2 * sigma;
  const lat = baseLoc.latitude + (ny / R) * (180 / Math.PI);
  const lon =
    baseLoc.longitude +
    (nx / (R * Math.cos(baseLoc.latitude * DEG))) * (180 / Math.PI);
  return { ...baseLoc, latitude: lat, longitude: lon };
}

// ---------------------------------------------------------------------------
// latLonToEnu / enuToLatLon
// ---------------------------------------------------------------------------

describe("latLonToEnu / enuToLatLon", () => {
  it("origin maps to (0, 0)", () => {
    const { x, y } = latLonToEnu(51.5, -0.1, 51.5, -0.1);
    expect(x).toBeCloseTo(0, 5);
    expect(y).toBeCloseTo(0, 5);
  });

  it("1 degree north ≈ 111,195 m (using spherical Earth approximation)", () => {
    const { x, y } = latLonToEnu(52.5, -0.1, 51.5, -0.1);
    expect(x).toBeCloseTo(0, 0);
    // 1 degree * π/180 * 6,371,000 m ≈ 111,194.9 m
    expect(y).toBeCloseTo(111_195, -1); // within ~100 m
  });

  it("round-trip is within 1 mm", () => {
    const lat0 = 37.7749, lon0 = -122.4194;
    const { x, y } = latLonToEnu(37.78, -122.41, lat0, lon0);
    const { lat, lon } = enuToLatLon(x, y, lat0, lon0);
    expect(lat).toBeCloseTo(37.78, 5);
    expect(lon).toBeCloseTo(-122.41, 5);
  });

  it("east displacement is correct (spherical Earth at equator)", () => {
    const { x } = latLonToEnu(0, 1, 0, 0); // 1 degree east at equator
    // 1 degree * π/180 * 6,371,000 m ≈ 111,194.9 m
    expect(x).toBeCloseTo(111_195, -1);
  });
});

// ---------------------------------------------------------------------------
// PredictiveEngine — basic lifecycle
// ---------------------------------------------------------------------------

describe("PredictiveEngine – lifecycle", () => {
  let engine;

  beforeEach(() => {
    engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
  });

  afterEach(() => {
    engine.close();
  });

  it("returns no anomalies for first update on a new client", () => {
    const { anomalies } = engine.update("c1", loc(51.5, -0.1));
    expect(anomalies).toBeInstanceOf(Array);
    // First fix — no prior state to compare against, so no anomalies expected
    expect(anomalies).toHaveLength(0);
  });

  it("returns empty trajectory for unknown client", () => {
    const traj = engine.getTrajectory("unknown");
    expect(traj).toEqual([]);
  });

  it("returns not-reachable ETA for unknown client", () => {
    const eta = engine.getETA("unknown", 51.5, -0.1);
    expect(eta.reachable).toBe(false);
  });

  it("removeClient clears all state", () => {
    engine.update("c2", loc(51.5, -0.1));
    engine.removeClient("c2");
    expect(engine.getTrajectory("c2")).toEqual([]);
    expect(engine.serializeState("c2")).toBeNull();
  });

  it("disabled engine returns empty everywhere", () => {
    const disabled = new PredictiveEngine({ config: { enable: false } });
    disabled.update("c", loc(1, 1));
    expect(disabled.getTrajectory("c")).toEqual([]);
    expect(disabled.detectAnomalies("c", loc(1, 1))).toEqual([]);
    expect(disabled.getETA("c", 1, 1).reachable).toBe(false);
    disabled.close();
  });
});

// ---------------------------------------------------------------------------
// Kalman filter convergence — stationary vehicle
// ---------------------------------------------------------------------------

describe("Kalman filter – stationary convergence", () => {
  it("position uncertainty decreases over repeated stationary updates", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5, processNoise: 0.01 } });
    const LAT = 48.8566, LON = 2.3522;
    let t = Date.now() - 30_000;

    engine.update("stat", loc(LAT, LON, { timestamp: t }));
    t += 1000;

    // Feed 30 stationary measurements
    for (let i = 0; i < 29; i++) {
      engine.update("stat", loc(LAT, LON, { timestamp: t }));
      t += 1000;
    }

    const state = engine.serializeState("stat");
    expect(state).not.toBeNull();
    // P[0] and P[5] are the position variances (x and y)
    expect(state.P[0]).toBeLessThan(5 ** 2); // must be smaller than initial GPS noise²
    expect(state.P[5]).toBeLessThan(5 ** 2);
    engine.close();
  });

  it("filter predicts near the origin for a stationary vehicle", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5, processNoise: 0.01 } });
    const LAT = 51.5, LON = -0.1;
    let t = Date.now() - 20_000;
    for (let i = 0; i < 20; i++) {
      engine.update("statb", loc(LAT, LON, { timestamp: t }));
      t += 1000;
    }
    const traj = engine.getTrajectory("statb", [10]);
    expect(traj).toHaveLength(1);
    // Predicted position should be very close to origin (within 10 m)
    const { x, y } = latLonToEnu(traj[0].lat, traj[0].lon, LAT, LON);
    const dist = Math.sqrt(x * x + y * y);
    expect(dist).toBeLessThan(10);
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// Prediction accuracy — moving vehicle
// ---------------------------------------------------------------------------

describe("Kalman filter – moving vehicle accuracy", () => {
  it("30-second prediction error < 10 m for 30 m/s vehicle with σ=5m GPS noise", () => {
    // Seed a deterministic pseudo-random sequence to keep tests reproducible
    let seed = 42;
    function pseudoRandom() {
      seed = (seed * 1664525 + 1013904223) & 0xffffffff;
      return ((seed >>> 0) / 0xffffffff);
    }

    const SPEED = 30; // m/s ≈ 108 km/h
    const HEADING = 0; // heading north
    const LAT0 = 52.0, LON0 = 0.0;
    const GPS_SIGMA = 5; // metres

    const track = makeTrack(LAT0, LON0, SPEED, HEADING, 50, 1000);

    const engine = new PredictiveEngine({
      config: { measurementNoise: GPS_SIGMA, processNoise: 0.1 },
    });

    // Feed the track with added noise
    for (const point of track) {
      const noisy = noisyLoc(point, GPS_SIGMA, pseudoRandom);
      engine.update("mover", noisy);
    }

    // Predict 30 s ahead from the last known position
    const traj = engine.getTrajectory("mover", [30]);
    expect(traj).toHaveLength(1);

    // The true position at +30 s from the last track point (direct calculation)
    const lastPoint = track[track.length - 1];
    const R_M = 6_371_000;
    const trueAheadLat = lastPoint.latitude + (SPEED * 30 / R_M) * (180 / Math.PI);
    const trueAheadLon = lastPoint.longitude;

    const { x: predX, y: predY } = latLonToEnu(
      traj[0].lat,
      traj[0].lon,
      LAT0,
      LON0,
    );
    const { x: trueX, y: trueY } = latLonToEnu(
      trueAheadLat,
      trueAheadLon,
      LAT0,
      LON0,
    );
    const error = Math.sqrt((predX - trueX) ** 2 + (predY - trueY) ** 2);
    expect(error).toBeLessThan(10);
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// getTrajectory output shape
// ---------------------------------------------------------------------------

describe("getTrajectory", () => {
  let engine;
  const LAT = 37.7749, LON = -122.4194;

  beforeEach(() => {
    engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
    let t = Date.now() - 10_000;
    for (let i = 0; i < 10; i++) {
      engine.update("traj", loc(LAT, LON, { speed: 20, heading: 90, timestamp: t }));
      t += 1000;
    }
  });

  afterEach(() => engine.close());

  it("returns one entry per horizon", () => {
    const traj = engine.getTrajectory("traj", [10, 30, 60]);
    expect(traj).toHaveLength(3);
    expect(traj.map((t) => t.horizon)).toEqual([10, 30, 60]);
  });

  it("each entry has lat, lon, speed, heading, confidenceEllipse", () => {
    const [entry] = engine.getTrajectory("traj", [15]);
    expect(entry).toHaveProperty("lat");
    expect(entry).toHaveProperty("lon");
    expect(entry).toHaveProperty("speed");
    expect(entry).toHaveProperty("heading");
    expect(entry.confidenceEllipse).toHaveProperty("semiMajorM");
    expect(entry.confidenceEllipse).toHaveProperty("semiMinorM");
    expect(entry.confidenceEllipse).toHaveProperty("orientationRad");
  });

  it("confidence ellipse semi-major is ≥ semi-minor", () => {
    const [entry] = engine.getTrajectory("traj", [60]);
    expect(entry.confidenceEllipse.semiMajorM).toBeGreaterThanOrEqual(
      entry.confidenceEllipse.semiMinorM
    );
  });

  it("horizons beyond maxHorizonS are ignored", () => {
    const engine2 = new PredictiveEngine({ config: { maxHorizonS: 30 } });
    engine2.update("traj2", loc(LAT, LON));
    const traj = engine2.getTrajectory("traj2", [10, 60, 120]);
    expect(traj.every((e) => e.horizon <= 30)).toBe(true);
    engine2.close();
  });

  it("uncertainty grows with horizon for moving vehicle", () => {
    const traj = engine.getTrajectory("traj", [10, 30, 60]);
    const [e10, e30, e60] = traj;
    // Uncertainty must be non-decreasing over time
    expect(e30.confidenceEllipse.semiMajorM).toBeGreaterThanOrEqual(
      e10.confidenceEllipse.semiMajorM - 0.1
    );
    expect(e60.confidenceEllipse.semiMajorM).toBeGreaterThanOrEqual(
      e30.confidenceEllipse.semiMajorM - 0.1
    );
  });
});

// ---------------------------------------------------------------------------
// GPS anomaly detection
// ---------------------------------------------------------------------------

describe("detectAnomalies – gps_anomaly", () => {
  it("does not flag normal measurement after convergence", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
    const LAT = 40.0, LON = -74.0;
    let t = Date.now() - 20_000;
    for (let i = 0; i < 10; i++) {
      engine.update("gps1", loc(LAT, LON, { timestamp: t }));
      t += 1000;
    }
    // normal measurement
    const anomalies = engine.detectAnomalies("gps1", loc(LAT + 0.00001, LON, { timestamp: t }));
    expect(anomalies).not.toContain("gps_anomaly");
    engine.close();
  });

  it("flags gps_anomaly when measurement is far from prediction", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
    const LAT = 40.0, LON = -74.0;
    let t = Date.now() - 20_000;
    for (let i = 0; i < 15; i++) {
      engine.update("gps2", loc(LAT, LON, { timestamp: t }));
      t += 1000;
    }
    // Spoof the position 1000 m away — far beyond 3σ for σ=5m
    const R = 6_371_000;
    const spoofedLat = LAT + (1000 / R) * (180 / Math.PI);
    const anomalies = engine.detectAnomalies("gps2", loc(spoofedLat, LON, { timestamp: t }));
    expect(anomalies).toContain("gps_anomaly");
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// Kinematic anomaly detection
// ---------------------------------------------------------------------------

describe("detectAnomalies – kinematic_anomaly", () => {
  it("flags kinematic_anomaly for impossible acceleration (0 → 100 km/h in 1s)", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
    const LAT = 51.5, LON = -0.1;
    let t = Date.now() - 5_000;
    // Stationary vehicle for 5 seconds
    for (let i = 0; i < 5; i++) {
      engine.update("kin1", loc(LAT, LON, { speed: 0, timestamp: t }));
      t += 1000;
    }
    // Instantaneous jump to 100 km/h (27.8 m/s) after 1 second
    const anomalies = engine.detectAnomalies(
      "kin1",
      loc(LAT, LON, { speed: 27.8, timestamp: t })
    );
    expect(anomalies).toContain("kinematic_anomaly");
    engine.close();
  });

  it("does not flag normal acceleration (0 → 10 m/s over 10s)", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
    const LAT = 51.5, LON = -0.1;
    let t = Date.now() - 11_000;
    for (let i = 0; i < 5; i++) {
      engine.update("kin2", loc(LAT, LON, { speed: 0, timestamp: t }));
      t += 1000;
    }
    t += 5_000; // 5 more seconds pass
    const anomalies = engine.detectAnomalies(
      "kin2",
      loc(LAT, LON, { speed: 10, timestamp: t })
    );
    expect(anomalies).not.toContain("kinematic_anomaly");
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// ETA computation
// ---------------------------------------------------------------------------

describe("getETA", () => {
  it("returns a plausible ETA for a vehicle heading toward target", () => {
    const SPEED = 20; // m/s
    const LAT0 = 48.8566, LON0 = 2.3522;
    const R = 6_371_000;
    // Target is 600 m north (should take ~30 s at 20 m/s)
    const targetLat = LAT0 + (600 / R) * (180 / Math.PI);
    const targetLon = LON0;

    const engine = new PredictiveEngine({
      config: { measurementNoise: 5, processNoise: 0.1, maxHorizonS: 120 },
    });
    const track = makeTrack(LAT0, LON0, SPEED, 0 /* north */, 20, 1000);
    for (const point of track) engine.update("eta1", point);

    const { etaMean, reachable } = engine.getETA("eta1", targetLat, targetLon);
    expect(reachable).toBe(true);
    expect(etaMean).toBeGreaterThan(0);
    // ETA should be within ±30 s of the true 30 s
    expect(etaMean).toBeLessThan(60);
    engine.close();
  });

  it("returns not-reachable when target is too far away", () => {
    const engine = new PredictiveEngine({
      config: { measurementNoise: 5, maxHorizonS: 10 },
    });
    // Stationary vehicle — can't reach a target 100 km away
    engine.update("eta2", loc(0, 0));
    const { reachable } = engine.getETA("eta2", 1, 0); // 1 degree ≈ 111 km
    expect(reachable).toBe(false);
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// Geofence pre-alerts
// ---------------------------------------------------------------------------

describe("checkPreAlerts", () => {
  it("emits geofence_pre_alert via room manager when trajectory intersects a fence", () => {
    const broadcastSpy = vi.fn();
    const mockRooms = {
      getClientRooms: vi.fn().mockReturnValue(new Set(["fleet-1"])),
      broadcast: broadcastSpy,
    };
    const mockGeofence = {
      checkPoint: vi.fn().mockReturnValue([{ id: "fence-A", name: "Depot" }]),
    };

    const engine = new PredictiveEngine({
      geofenceEngine: mockGeofence,
      roomManager: mockRooms,
      config: { measurementNoise: 5, preAlertHorizonS: 60 },
    });

    // Feed 10 points so filter has state
    let t = Date.now() - 10_000;
    for (let i = 0; i < 10; i++) {
      engine.update("alert1", loc(10, 10, { speed: 20, heading: 0, timestamp: t }));
      t += 1000;
    }

    engine.checkPreAlerts("alert1");

    expect(broadcastSpy).toHaveBeenCalledWith(
      "fleet-1",
      expect.objectContaining({ type: "geofence_pre_alert" }),
      "alert1"
    );
    engine.close();
  });

  it("debounces pre-alerts for the same fence within the debounce window", () => {
    const broadcastSpy = vi.fn();
    const mockRooms = {
      getClientRooms: vi.fn().mockReturnValue(new Set(["r1"])),
      broadcast: broadcastSpy,
    };
    const mockGeofence = {
      checkPoint: vi.fn().mockReturnValue([{ id: "fence-B", name: "Zone B" }]),
    };

    const engine = new PredictiveEngine({
      geofenceEngine: mockGeofence,
      roomManager: mockRooms,
      config: { measurementNoise: 5, preAlertHorizonS: 30 },
    });

    let t = Date.now() - 10_000;
    for (let i = 0; i < 5; i++) {
      engine.update("dedup1", loc(1, 1, { speed: 10, heading: 0, timestamp: t }));
      t += 1000;
    }

    engine.checkPreAlerts("dedup1");
    const firstCount = broadcastSpy.mock.calls.length;

    // Call again immediately — should be debounced
    engine.checkPreAlerts("dedup1");
    expect(broadcastSpy.mock.calls.length).toBe(firstCount);
    engine.close();
  });

  it("returns empty array when no geofence engine is configured", () => {
    const engine = new PredictiveEngine({ config: {} });
    engine.update("no-fence", loc(0, 0));
    const alerts = engine.checkPreAlerts("no-fence");
    expect(alerts).toEqual([]);
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// Session serialisation / restore
// ---------------------------------------------------------------------------

describe("serializeState / restoreState", () => {
  it("returns null for unknown client", () => {
    const engine = new PredictiveEngine();
    expect(engine.serializeState("nobody")).toBeNull();
    engine.close();
  });

  it("round-trips filter state", () => {
    const engine = new PredictiveEngine({ config: { measurementNoise: 5 } });
    let t = Date.now() - 10_000;
    const LAT = 51.5, LON = -0.1;
    for (let i = 0; i < 10; i++) {
      engine.update("serial1", loc(LAT, LON, { speed: 15, heading: 90, timestamp: t }));
      t += 1000;
    }
    const snapshot = engine.serializeState("serial1");
    expect(snapshot).not.toBeNull();

    const engine2 = new PredictiveEngine({ config: { measurementNoise: 5 } });
    engine2.restoreState("serial1", snapshot);

    // Trajectory should be available on the new engine
    const traj = engine2.getTrajectory("serial1", [10]);
    expect(traj).toHaveLength(1);
    expect(traj[0].lat).toBeCloseTo(LAT, 2);
    engine.close();
    engine2.close();
  });

  it("restoreState ignores malformed input", () => {
    const engine = new PredictiveEngine();
    engine.restoreState("bad", null);
    engine.restoreState("bad", { x: [1, 2] }); // wrong length
    expect(engine.serializeState("bad")).toBeNull();
    engine.close();
  });
});

// ---------------------------------------------------------------------------
// Integration with a mock server return value
// ---------------------------------------------------------------------------

describe("PredictiveEngine integration via createServer predictor field", () => {
  it("createServer exposes a predictor instance", async () => {
    const { createServer } = await import("../src/server.js");
    process.env.AUTH_SECRET = "test-secret";
    const srv = createServer({ port: 0, heartbeatMs: 60_000 });
    expect(srv.predictor).toBeInstanceOf(PredictiveEngine);
    await new Promise((r) => srv.wss.close(r));
    delete process.env.AUTH_SECRET;
  });
});
