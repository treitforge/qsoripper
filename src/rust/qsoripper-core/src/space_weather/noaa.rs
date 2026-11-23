//! NOAA SWPC current space weather provider.

use std::{env, fmt, time::Duration};

use prost_types::Timestamp;
use reqwest::Client;
use serde::Deserialize;

use crate::proto::qsoripper::domain::{SpaceWeatherSnapshot, SpaceWeatherStatus};

use super::provider::{SpaceWeatherProvider, SpaceWeatherProviderError};

/// Environment variable that enables or disables NOAA current space weather fetching.
pub const NOAA_SPACE_WEATHER_ENABLED_ENV_VAR: &str = "QSORIPPER_NOAA_SPACE_WEATHER_ENABLED";
/// Environment variable that overrides the NOAA planetary K-index JSON URL.
pub const NOAA_KP_INDEX_URL_ENV_VAR: &str = "QSORIPPER_NOAA_KP_INDEX_URL";
/// Environment variable that overrides the NOAA daily solar indices text URL.
pub const NOAA_SOLAR_INDICES_URL_ENV_VAR: &str = "QSORIPPER_NOAA_SOLAR_INDICES_URL";
/// Environment variable that overrides the NOAA HTTP timeout in seconds.
pub const NOAA_HTTP_TIMEOUT_SECONDS_ENV_VAR: &str = "QSORIPPER_NOAA_HTTP_TIMEOUT_SECONDS";
/// Environment variable that overrides the snapshot refresh interval in seconds.
pub const NOAA_REFRESH_INTERVAL_SECONDS_ENV_VAR: &str = "QSORIPPER_NOAA_REFRESH_INTERVAL_SECONDS";
/// Environment variable that overrides the stale-after threshold in seconds.
pub const NOAA_STALE_AFTER_SECONDS_ENV_VAR: &str = "QSORIPPER_NOAA_STALE_AFTER_SECONDS";

/// Default NOAA planetary K-index endpoint.
pub const DEFAULT_NOAA_KP_INDEX_URL: &str =
    "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json";
/// Default NOAA daily solar indices endpoint.
pub const DEFAULT_NOAA_SOLAR_INDICES_URL: &str =
    "https://services.swpc.noaa.gov/text/daily-solar-indices.txt";
/// Default NOAA HTTP timeout.
pub const DEFAULT_NOAA_HTTP_TIMEOUT_SECONDS: u64 = 8;
/// Default refresh interval for current space weather snapshots.
pub const DEFAULT_NOAA_REFRESH_INTERVAL_SECONDS: u64 = 900;
/// Default stale-after threshold for current space weather snapshots.
pub const DEFAULT_NOAA_STALE_AFTER_SECONDS: u64 = 3600;

/// NOAA current space weather provider configuration.
#[derive(Clone)]
pub struct NoaaSpaceWeatherConfig {
    enabled: bool,
    kp_index_url: String,
    solar_indices_url: String,
    http_timeout: Duration,
    refresh_interval: Duration,
    stale_after: Duration,
}

impl fmt::Debug for NoaaSpaceWeatherConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NoaaSpaceWeatherConfig")
            .field("enabled", &self.enabled)
            .field("kp_index_url", &self.kp_index_url)
            .field("solar_indices_url", &self.solar_indices_url)
            .field("http_timeout", &self.http_timeout)
            .field("refresh_interval", &self.refresh_interval)
            .field("stale_after", &self.stale_after)
            .finish()
    }
}

impl NoaaSpaceWeatherConfig {
    /// Load provider configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns `NoaaSpaceWeatherConfigError` when integer-valued settings
    /// cannot be parsed.
    pub fn from_env() -> Result<Self, NoaaSpaceWeatherConfigError> {
        Self::from_value_provider(|name| env::var(name).ok())
    }

    /// Load provider configuration from an arbitrary key/value source.
    ///
    /// # Errors
    ///
    /// Returns `NoaaSpaceWeatherConfigError` when integer-valued settings
    /// cannot be parsed.
    pub fn from_value_provider<F>(mut get_value: F) -> Result<Self, NoaaSpaceWeatherConfigError>
    where
        F: FnMut(&'static str) -> Option<String>,
    {
        let enabled =
            optional_value_bool(NOAA_SPACE_WEATHER_ENABLED_ENV_VAR, true, &mut get_value)?;
        let kp_index_url = optional_value(NOAA_KP_INDEX_URL_ENV_VAR, &mut get_value)
            .unwrap_or_else(|| DEFAULT_NOAA_KP_INDEX_URL.to_string());
        let solar_indices_url = optional_value(NOAA_SOLAR_INDICES_URL_ENV_VAR, &mut get_value)
            .unwrap_or_else(|| DEFAULT_NOAA_SOLAR_INDICES_URL.to_string());
        let http_timeout_seconds = optional_value_u64(
            NOAA_HTTP_TIMEOUT_SECONDS_ENV_VAR,
            DEFAULT_NOAA_HTTP_TIMEOUT_SECONDS,
            &mut get_value,
        )?;
        let refresh_interval_seconds = optional_value_u64(
            NOAA_REFRESH_INTERVAL_SECONDS_ENV_VAR,
            DEFAULT_NOAA_REFRESH_INTERVAL_SECONDS,
            &mut get_value,
        )?;
        let stale_after_seconds = optional_value_u64(
            NOAA_STALE_AFTER_SECONDS_ENV_VAR,
            DEFAULT_NOAA_STALE_AFTER_SECONDS,
            &mut get_value,
        )?;

        Ok(Self {
            enabled,
            kp_index_url,
            solar_indices_url,
            http_timeout: Duration::from_secs(http_timeout_seconds),
            refresh_interval: Duration::from_secs(refresh_interval_seconds),
            stale_after: Duration::from_secs(stale_after_seconds),
        })
    }

    /// Return whether space weather fetching is enabled.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Return the refresh interval for cached snapshots.
    #[must_use]
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    /// Return the stale-after threshold for cached snapshots.
    #[must_use]
    pub fn stale_after(&self) -> Duration {
        self.stale_after
    }
}

/// Errors while reading NOAA provider configuration.
#[derive(Debug, thiserror::Error)]
pub enum NoaaSpaceWeatherConfigError {
    /// An environment variable could not be parsed as an integer.
    #[error("Environment variable '{name}' has invalid integer value '{value}'.")]
    InvalidInteger {
        /// Environment variable name.
        name: &'static str,
        /// Raw value that failed integer parsing.
        value: String,
    },
    /// An environment variable could not be parsed as a boolean.
    #[error("Environment variable '{name}' has invalid boolean value '{value}'.")]
    InvalidBoolean {
        /// Environment variable name.
        name: &'static str,
        /// Raw value that failed boolean parsing.
        value: String,
    },
}

/// NOAA SWPC provider implementation.
#[derive(Debug)]
pub struct NoaaSpaceWeatherProvider {
    config: NoaaSpaceWeatherConfig,
    client: Client,
}

impl NoaaSpaceWeatherProvider {
    /// Create a provider using validated configuration.
    ///
    /// # Errors
    ///
    /// Returns `SpaceWeatherProviderError::transport` when the HTTP client
    /// cannot be created.
    pub fn new(config: NoaaSpaceWeatherConfig) -> Result<Self, SpaceWeatherProviderError> {
        let client = Client::builder()
            .timeout(config.http_timeout)
            .build()
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "Failed to create NOAA HTTP client: {error}"
                ))
            })?;

        Ok(Self { config, client })
    }

    async fn fetch_kp_data(&self) -> Result<PlanetaryKIndexEntry, SpaceWeatherProviderError> {
        let body = self
            .client
            .get(self.config.kp_index_url.as_str())
            .send()
            .await
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "Failed to fetch NOAA planetary K-index data: {error}"
                ))
            })?
            .error_for_status()
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "NOAA planetary K-index request failed: {error}"
                ))
            })?
            .text()
            .await
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "Failed to read NOAA planetary K-index response: {error}"
                ))
            })?;
        let entries =
            serde_json::from_str::<Vec<PlanetaryKIndexEntry>>(&body).map_err(|error| {
                SpaceWeatherProviderError::parse(format!(
                    "Failed to parse NOAA planetary K-index JSON: {error}"
                ))
            })?;
        entries.into_iter().last().ok_or_else(|| {
            SpaceWeatherProviderError::parse("NOAA planetary K-index feed returned no entries.")
        })
    }

    async fn fetch_solar_data(&self) -> Result<DailySolarIndices, SpaceWeatherProviderError> {
        let body = self
            .client
            .get(self.config.solar_indices_url.as_str())
            .send()
            .await
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "Failed to fetch NOAA daily solar indices: {error}"
                ))
            })?
            .error_for_status()
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "NOAA daily solar indices request failed: {error}"
                ))
            })?
            .text()
            .await
            .map_err(|error| {
                SpaceWeatherProviderError::transport(format!(
                    "Failed to read NOAA daily solar indices response: {error}"
                ))
            })?;
        parse_daily_solar_indices(&body)
    }
}

#[tonic::async_trait]
impl SpaceWeatherProvider for NoaaSpaceWeatherProvider {
    async fn fetch_current(&self) -> Result<SpaceWeatherSnapshot, SpaceWeatherProviderError> {
        if !self.config.enabled {
            return Err(SpaceWeatherProviderError::disabled(
                "NOAA space weather fetching is disabled.",
            ));
        }

        let (kp, solar) = tokio::try_join!(self.fetch_kp_data(), self.fetch_solar_data())?;

        Ok(SpaceWeatherSnapshot {
            observed_at: Some(parse_noaa_timestamp(&kp.time_tag)?),
            status: SpaceWeatherStatus::Current as i32,
            planetary_k_index: Some(kp.kp),
            planetary_a_index: Some(kp.a_running),
            solar_flux_index: Some(solar.solar_flux_index),
            sunspot_number: Some(solar.sunspot_number),
            geomagnetic_storm_scale: geomagnetic_storm_scale(kp.kp),
            source_name: Some("NOAA SWPC".to_string()),
            ..SpaceWeatherSnapshot::default()
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct PlanetaryKIndexEntry {
    time_tag: String,
    #[serde(rename = "Kp")]
    kp: f64,
    a_running: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct DailySolarIndices {
    solar_flux_index: f64,
    sunspot_number: u32,
}

fn geomagnetic_storm_scale(kp: f64) -> Option<u32> {
    if kp >= 9.0 {
        Some(5)
    } else if kp >= 8.0 {
        Some(4)
    } else if kp >= 7.0 {
        Some(3)
    } else if kp >= 6.0 {
        Some(2)
    } else if kp >= 5.0 {
        Some(1)
    } else {
        None
    }
}

fn parse_noaa_timestamp(raw: &str) -> Result<Timestamp, SpaceWeatherProviderError> {
    let timestamp = time::PrimitiveDateTime::parse(
        raw,
        time::macros::format_description!("[year]-[month padding:none]-[day padding:none]T[hour padding:none]:[minute padding:none]:[second padding:none]"),
    )
    .map_err(|error| {
        SpaceWeatherProviderError::parse(format!(
            "Failed to parse NOAA K-index timestamp '{raw}': {error}"
        ))
    })?;
    Ok(crate::utc::to_timestamp(timestamp.assume_utc()))
}

fn parse_daily_solar_indices(body: &str) -> Result<DailySolarIndices, SpaceWeatherProviderError> {
    let line = body
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| line.chars().next().is_some_and(|c| c.is_ascii_digit()))
        .ok_or_else(|| {
            SpaceWeatherProviderError::parse(
                "NOAA daily solar indices feed returned no data lines.",
            )
        })?;
    let columns: Vec<_> = line.split_whitespace().collect();
    if columns.len() < 5 {
        return Err(SpaceWeatherProviderError::parse(format!(
            "NOAA daily solar indices line was missing expected columns: '{line}'"
        )));
    }

    let year_raw = required_column(&columns, 0, line)?;
    let month_raw = required_column(&columns, 1, line)?;
    let day_raw = required_column(&columns, 2, line)?;
    let solar_flux_raw = required_column(&columns, 3, line)?;
    let sunspot_raw = required_column(&columns, 4, line)?;

    let year = year_raw.parse::<i32>().map_err(|error| {
        SpaceWeatherProviderError::parse(format!(
            "Failed to parse NOAA solar indices year '{year_raw}': {error}"
        ))
    })?;
    let month = month_raw.parse::<u8>().map_err(|error| {
        SpaceWeatherProviderError::parse(format!(
            "Failed to parse NOAA solar indices month '{month_raw}': {error}"
        ))
    })?;
    let day = day_raw.parse::<u8>().map_err(|error| {
        SpaceWeatherProviderError::parse(format!(
            "Failed to parse NOAA solar indices day '{day_raw}': {error}"
        ))
    })?;
    let solar_flux_index = solar_flux_raw.parse::<f64>().map_err(|error| {
        SpaceWeatherProviderError::parse(format!(
            "Failed to parse NOAA solar flux '{solar_flux_raw}': {error}"
        ))
    })?;
    let sunspot_number = sunspot_raw.parse::<u32>().map_err(|error| {
        SpaceWeatherProviderError::parse(format!(
            "Failed to parse NOAA sunspot number '{sunspot_raw}': {error}"
        ))
    })?;

    time::Month::try_from(month)
        .and_then(|month| time::Date::from_calendar_date(year, month, day))
        .map_err(|_| {
            SpaceWeatherProviderError::parse(format!(
                "NOAA daily solar indices date '{year:04}-{month:02}-{day:02}' is invalid."
            ))
        })?;

    Ok(DailySolarIndices {
        solar_flux_index,
        sunspot_number,
    })
}

fn required_column<'a>(
    columns: &'a [&str],
    index: usize,
    line: &str,
) -> Result<&'a str, SpaceWeatherProviderError> {
    columns.get(index).copied().ok_or_else(|| {
        SpaceWeatherProviderError::parse(format!(
            "NOAA daily solar indices line was missing column {index}: '{line}'"
        ))
    })
}

fn optional_value<F>(name: &'static str, get_value: &mut F) -> Option<String>
where
    F: FnMut(&'static str) -> Option<String>,
{
    get_value(name).and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn optional_value_u64<F>(
    name: &'static str,
    default_value: u64,
    get_value: &mut F,
) -> Result<u64, NoaaSpaceWeatherConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    optional_value(name, get_value).map_or(Ok(default_value), |value| {
        value
            .parse::<u64>()
            .map_err(|_| NoaaSpaceWeatherConfigError::InvalidInteger { name, value })
    })
}

fn optional_value_bool<F>(
    name: &'static str,
    default_value: bool,
    get_value: &mut F,
) -> Result<bool, NoaaSpaceWeatherConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    optional_value(name, get_value).map_or(Ok(default_value), |value| {
        match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => Ok(true),
            "0" | "false" | "no" | "n" | "off" => Ok(false),
            _ => Err(NoaaSpaceWeatherConfigError::InvalidBoolean { name, value }),
        }
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_to_enabled_and_known_urls() {
        let config = NoaaSpaceWeatherConfig::from_value_provider(|_| None).expect("config");

        assert!(config.enabled());
        assert_eq!(
            Duration::from_secs(DEFAULT_NOAA_REFRESH_INTERVAL_SECONDS),
            config.refresh_interval()
        );
        assert_eq!(
            Duration::from_secs(DEFAULT_NOAA_STALE_AFTER_SECONDS),
            config.stale_after()
        );
    }

    #[test]
    fn parse_daily_solar_indices_reads_last_data_row() {
        let body = "\
# header\n\
2026 04 11   93     42      180      0    -999      *   2  0  0  1  0  0  0\n\
2026 04 12   99     47      270      1    -999      *   6  0  0  0  0  0  0\n";

        let parsed = parse_daily_solar_indices(body).expect("parsed solar data");

        assert_eq!(
            DailySolarIndices {
                solar_flux_index: 99.0,
                sunspot_number: 47
            },
            parsed
        );
    }

    #[test]
    fn parse_noaa_timestamp_accepts_k_index_timestamp() {
        let timestamp = parse_noaa_timestamp("2026-04-13T18:00:00").expect("timestamp");

        assert_eq!(1_776_103_200, timestamp.seconds);
    }

    #[test]
    fn geomagnetic_storm_scale_is_empty_below_g1() {
        assert_eq!(None, geomagnetic_storm_scale(4.67));
        assert_eq!(Some(1), geomagnetic_storm_scale(5.0));
        assert_eq!(Some(5), geomagnetic_storm_scale(9.0));
    }
}
