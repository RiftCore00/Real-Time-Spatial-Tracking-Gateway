-- 001_create_locations.sql
--
-- Creates the `locations` table that src/storage.js (PostgresAdapter) writes
-- every persisted `location_update` into via saveLocation(clientId, roomId,
-- payload). Apply with, e.g.:
--
--   psql "$DATABASE_URL" -f db/migrations/001_create_locations.sql

CREATE TABLE IF NOT EXISTS locations (
  id          UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id   TEXT             NOT NULL,
  room_id     TEXT             NOT NULL,
  latitude    DOUBLE PRECISION NOT NULL,
  longitude   DOUBLE PRECISION NOT NULL,
  altitude    DOUBLE PRECISION,
  accuracy    DOUBLE PRECISION,
  speed       DOUBLE PRECISION,
  recorded_at TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS locations_room_id_recorded_at_idx
  ON locations (room_id, recorded_at DESC);
