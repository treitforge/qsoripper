//! Core Rust engine modules for `QsoRipper`.
#![deny(unsafe_code)]

/// ADIF import/export adapters.
pub mod adif;
/// Application services that coordinate engine workflows above storage ports.
pub mod application;
/// Contest calendar providers, caching, and filtering.
pub mod contest_calendar;
/// CW keying macro expansion and engine-side keyer adapters.
pub mod cw;
/// Domain helpers for QSO and lookup-related types.
pub mod domain;
/// FFI boundary for DSP helpers.
pub mod ffi;
/// Spherical-Earth geodesy: distance, bearing, sampling, Maidenhead resolution.
pub mod geodesy;
/// Lookup orchestration, providers, and cache policy.
pub mod lookup;
/// ARRL Logbook of the World upload and confirmation synchronization.
pub mod lotw;
/// Generated protobuf and gRPC bindings.
pub mod proto;
/// QRZ Logbook API client for bidirectional QSO sync.
pub mod qrz_logbook;
/// Rig control providers, normalization, and cached rig state snapshots.
pub mod rig_control;
/// Space weather providers, caching, and normalization.
pub mod space_weather;
/// Storage ports, errors, and query types for engine-owned persistence.
pub mod storage;
/// UTC timestamp conversion and display helpers.
pub mod utc;
/// WSJT-X ingestion helpers that feed the ADIF import path.
pub mod wsjtx;
