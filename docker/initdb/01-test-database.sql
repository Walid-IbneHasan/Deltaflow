-- Scratch database for the integration tests, so they can drop and recreate
-- tables without touching the data `make seed` / `make load` work on.
CREATE DATABASE deltaflow_test;
