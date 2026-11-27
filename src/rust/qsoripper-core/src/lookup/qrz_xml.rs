//! QRZ XML lookup provider adapter.

use std::{
    env, fmt,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use prost_types::Timestamp;
use reqwest::{header::HeaderMap, Client, Request};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    domain::lookup::normalize_callsign,
    proto::qsoripper::domain::{
        CallsignRecord, DebugHttpExchange, DebugHttpHeader, GeoSource, QslPreference,
    },
};

use super::provider::{CallsignProvider, ProviderLookup, ProviderLookupError};

/// Environment variable that overrides the QRZ XML base URL.
pub const QRZ_XML_BASE_URL_ENV_VAR: &str = "QSORIPPER_QRZ_XML_BASE_URL";
/// Environment variable that provides the QRZ XML username.
pub const QRZ_XML_USERNAME_ENV_VAR: &str = "QSORIPPER_QRZ_XML_USERNAME";
/// Environment variable that provides the QRZ XML password.
pub const QRZ_XML_PASSWORD_ENV_VAR: &str = "QSORIPPER_QRZ_XML_PASSWORD";
/// Environment variable that provides the QRZ request user agent.
pub const QRZ_USER_AGENT_ENV_VAR: &str = "QSORIPPER_QRZ_USER_AGENT";
/// Environment variable that overrides the QRZ HTTP timeout in seconds.
pub const QRZ_HTTP_TIMEOUT_SECONDS_ENV_VAR: &str = "QSORIPPER_QRZ_HTTP_TIMEOUT_SECONDS";
/// Environment variable that overrides the QRZ retry count.
pub const QRZ_MAX_RETRIES_ENV_VAR: &str = "QSORIPPER_QRZ_MAX_RETRIES";
/// Environment variable that enables request-capture mode instead of live calls.
pub const QRZ_XML_CAPTURE_ONLY_ENV_VAR: &str = "QSORIPPER_QRZ_XML_CAPTURE_ONLY";

/// Default QRZ XML endpoint.
pub const DEFAULT_QRZ_XML_BASE_URL: &str = "https://xmldata.qrz.com/xml/current/";
/// Default QRZ HTTP timeout in seconds.
pub const DEFAULT_QRZ_HTTP_TIMEOUT_SECONDS: u64 = 8;
/// Default retry count for retryable QRZ failures.
pub const DEFAULT_QRZ_MAX_RETRIES: u32 = 2;
const RETRY_BASE_DELAY_MILLIS: u64 = 200;

/// QRZ XML provider configuration.
#[derive(Clone)]
pub struct QrzXmlConfig {
    base_url: String,
    username: String,
    password: String,
    user_agent: String,
    http_timeout: Duration,
    max_retries: u32,
    capture_only: bool,
}

impl fmt::Debug for QrzXmlConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QrzXmlConfig")
            .field("base_url", &self.base_url)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("user_agent", &self.user_agent)
            .field("http_timeout", &self.http_timeout)
            .field("max_retries", &self.max_retries)
            .field("capture_only", &self.capture_only)
            .finish()
    }
}

impl QrzXmlConfig {
    /// Load provider configuration from environment variables.
    ///
    /// Required variables:
    /// - `QSORIPPER_QRZ_XML_USERNAME`
    /// - `QSORIPPER_QRZ_XML_PASSWORD`
    /// - `QSORIPPER_QRZ_USER_AGENT`
    ///
    /// Optional variables:
    /// - `QSORIPPER_QRZ_XML_BASE_URL` (default: QRZ current endpoint)
    /// - `QSORIPPER_QRZ_HTTP_TIMEOUT_SECONDS` (default: 8)
    /// - `QSORIPPER_QRZ_MAX_RETRIES` (default: 2)
    /// - `QSORIPPER_QRZ_XML_CAPTURE_ONLY` (default: false)
    ///
    /// # Errors
    ///
    /// Returns `QrzXmlConfigError` when required values are missing/blank or
    /// integer-valued settings cannot be parsed.
    pub fn from_env() -> Result<Self, QrzXmlConfigError> {
        Self::from_value_provider(|name| env::var(name).ok())
    }

    /// Load provider configuration from an arbitrary key/value source.
    ///
    /// This is used by the developer runtime-config surface so a running engine
    /// can be reconfigured without mutating the process environment.
    ///
    /// # Errors
    ///
    /// Returns `QrzXmlConfigError` when required values are missing/blank or
    /// integer-valued settings cannot be parsed.
    pub fn from_value_provider<F>(mut get_value: F) -> Result<Self, QrzXmlConfigError>
    where
        F: FnMut(&'static str) -> Option<String>,
    {
        let base_url = optional_value(QRZ_XML_BASE_URL_ENV_VAR, &mut get_value)
            .unwrap_or_else(|| DEFAULT_QRZ_XML_BASE_URL.to_string());
        let username = required_value(QRZ_XML_USERNAME_ENV_VAR, &mut get_value)?;
        let password = required_value(QRZ_XML_PASSWORD_ENV_VAR, &mut get_value)?;
        let user_agent = required_value(QRZ_USER_AGENT_ENV_VAR, &mut get_value)?;
        let http_timeout_seconds = optional_value_u64(
            QRZ_HTTP_TIMEOUT_SECONDS_ENV_VAR,
            DEFAULT_QRZ_HTTP_TIMEOUT_SECONDS,
            &mut get_value,
        )?;
        let max_retries = optional_value_u32(
            QRZ_MAX_RETRIES_ENV_VAR,
            DEFAULT_QRZ_MAX_RETRIES,
            &mut get_value,
        )?;
        let capture_only =
            optional_value_bool(QRZ_XML_CAPTURE_ONLY_ENV_VAR, false, &mut get_value)?;

        Ok(Self {
            base_url: normalize_base_url(base_url),
            username,
            password,
            user_agent,
            http_timeout: Duration::from_secs(http_timeout_seconds),
            max_retries,
            capture_only,
        })
    }

    /// Return the configured QRZ XML endpoint for diagnostics.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Return whether request-capture mode is enabled.
    #[must_use]
    pub fn capture_only(&self) -> bool {
        self.capture_only
    }
}

/// Errors while reading QRZ provider configuration.
#[derive(Debug, thiserror::Error)]
pub enum QrzXmlConfigError {
    /// A required environment variable is missing.
    #[error("Required environment variable '{name}' is missing or blank.")]
    Missing {
        /// Environment variable name.
        name: &'static str,
    },
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

/// QRZ XML provider implementation.
#[derive(Debug)]
pub struct QrzXmlProvider {
    config: QrzXmlConfig,
    client: Client,
    session_key: Mutex<Option<String>>,
}

impl QrzXmlProvider {
    /// Create a provider using validated configuration.
    ///
    /// # Errors
    ///
    /// Returns `ProviderLookupError::configuration` when the HTTP client cannot
    /// be created.
    pub fn new(config: QrzXmlConfig) -> Result<Self, ProviderLookupError> {
        let client = Client::builder()
            .user_agent(config.user_agent.clone())
            .timeout(config.http_timeout)
            .build()
            .map_err(|error| {
                ProviderLookupError::configuration(format!(
                    "Failed to create QRZ HTTP client: {error}"
                ))
            })?;

        Ok(Self {
            config,
            client,
            session_key: Mutex::new(None),
        })
    }

    /// Test credentials by performing a login attempt.
    ///
    /// Returns `Ok(())` when the login succeeds and a session key is obtained.
    ///
    /// # Errors
    ///
    /// Returns `ProviderLookupError` when authentication fails or the network
    /// request cannot be completed.
    pub async fn test_login(&self) -> Result<(), ProviderLookupError> {
        let (_key, _exchanges) = self.login().await?;
        Ok(())
    }

    async fn ensure_session_key(
        &self,
    ) -> Result<(String, Vec<DebugHttpExchange>), ProviderLookupError> {
        if let Some(existing) = self.session_key.lock().await.clone() {
            return Ok((existing, Vec::new()));
        }

        self.login().await
    }

    async fn login(&self) -> Result<(String, Vec<DebugHttpExchange>), ProviderLookupError> {
        let (response, debug_http_exchanges) = self
            .request_database(
                &[
                    ("username", self.config.username.clone()),
                    ("password", self.config.password.clone()),
                    ("agent", self.config.user_agent.clone()),
                ],
                "login",
            )
            .await?;

        let session = response.session.ok_or_else(|| {
            ProviderLookupError::parse(
                "QRZ login response did not include a <Session> element.".to_string(),
                debug_http_exchanges.clone(),
            )
        })?;

        if let Some(error) = session.error {
            return Err(ProviderLookupError::authentication(
                format!("QRZ login failed: {error}"),
                debug_http_exchanges,
            ));
        }

        let key = session.key.ok_or_else(|| {
            ProviderLookupError::authentication(
                "QRZ login response did not include a session key.".to_string(),
                debug_http_exchanges.clone(),
            )
        })?;

        self.store_session_key(&key).await;
        Ok((key, debug_http_exchanges))
    }

    async fn store_session_key(&self, key: &str) {
        *self.session_key.lock().await = Some(key.to_string());
    }

    async fn clear_session_key(&self) {
        *self.session_key.lock().await = None;
    }

    async fn request_database(
        &self,
        query: &[(&str, String)],
        operation: &'static str,
    ) -> Result<(QrzDatabase, Vec<DebugHttpExchange>), ProviderLookupError> {
        let CapturedQrzResponse {
            body,
            debug_http_exchanges,
        } = self.send_request(query, operation).await?;
        let database = quick_xml::de::from_str::<QrzDatabase>(&body).map_err(|error| {
            ProviderLookupError::parse(
                format!("Failed to parse QRZ XML response: {error}"),
                debug_http_exchanges.clone(),
            )
        })?;
        Ok((database, debug_http_exchanges))
    }

    fn capture_request_message(request: &Request, query: &[(&str, String)]) -> String {
        let query_details = query
            .iter()
            .map(|(name, value)| render_capture_query(name, value))
            .collect::<Vec<_>>()
            .join("; ");
        format!(
            "QRZ XML request capture mode enabled ({QRZ_XML_CAPTURE_ONLY_ENV_VAR}=true): request not sent. QRZ XML uses HTTP GET query parameters (no JSON body). method={}, url={}, query_details=[{}]",
            request.method(),
            redact_request_url(request),
            query_details
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "QRZ request execution owns retry, redaction, and capture as one provider-edge flow."
    )]
    async fn send_request(
        &self,
        query: &[(&str, String)],
        operation: &'static str,
    ) -> Result<CapturedQrzResponse, ProviderLookupError> {
        let mut debug_http_exchanges = Vec::new();
        let mut attempt = 0_u32;

        loop {
            let request = self
                .client
                .get(&self.config.base_url)
                .query(query)
                .build()
                .map_err(|error| {
                    ProviderLookupError::configuration(format!(
                        "Failed to build QRZ HTTP request for diagnostics: {error}"
                    ))
                })?;
            let started_at = SystemTime::now();
            let started_at_utc = system_time_to_timestamp(started_at);
            let duration_start = Instant::now();
            let method = request.method().to_string();
            let url = redact_request_url(&request);
            let request_headers = capture_headers(request.headers());

            if self.config.capture_only {
                let exchange = DebugHttpExchange {
                    provider_name: "QRZ XML".to_string(),
                    operation: operation.to_string(),
                    started_at_utc: Some(started_at_utc),
                    duration_ms: duration_to_millis_u32(duration_start.elapsed()),
                    attempt: attempt + 1,
                    method,
                    url,
                    request_headers,
                    request_body: None,
                    response_status_code: None,
                    response_headers: Vec::new(),
                    response_body: None,
                    error_message: Some("request not sent (capture_only=true)".to_string()),
                };
                debug_http_exchanges.push(exchange);
                return Err(ProviderLookupError::transport(
                    Self::capture_request_message(&request, query),
                    debug_http_exchanges,
                ));
            }

            match self.client.execute(request).await {
                Ok(response) => {
                    let status = response.status();
                    let response_headers = capture_headers(response.headers());
                    let duration_ms = duration_to_millis_u32(duration_start.elapsed());
                    let response_body = response.text().await.map_err(|error| {
                        debug_http_exchanges.push(DebugHttpExchange {
                            provider_name: "QRZ XML".to_string(),
                            operation: operation.to_string(),
                            started_at_utc: Some(started_at_utc),
                            duration_ms,
                            attempt: attempt + 1,
                            method: method.clone(),
                            url: url.clone(),
                            request_headers: request_headers.clone(),
                            request_body: None,
                            response_status_code: Some(status.as_u16().into()),
                            response_headers: response_headers.clone(),
                            response_body: None,
                            error_message: Some(format!(
                                "Failed to read QRZ response body: {error}"
                            )),
                        });
                        ProviderLookupError::transport(
                            format!("Failed to read QRZ response body: {error}"),
                            debug_http_exchanges.clone(),
                        )
                    })?;
                    let exchange = DebugHttpExchange {
                        provider_name: "QRZ XML".to_string(),
                        operation: operation.to_string(),
                        started_at_utc: Some(started_at_utc),
                        duration_ms,
                        attempt: attempt + 1,
                        method: method.clone(),
                        url: url.clone(),
                        request_headers: request_headers.clone(),
                        request_body: None,
                        response_status_code: Some(status.as_u16().into()),
                        response_headers,
                        response_body: Some(redact_qrz_xml_response(&response_body)),
                        error_message: (!status.is_success()).then(|| format!("HTTP {status}")),
                    };
                    debug_http_exchanges.push(exchange);

                    if status.is_success() {
                        return Ok(CapturedQrzResponse {
                            body: response_body,
                            debug_http_exchanges,
                        });
                    }

                    if status.as_u16() == 429 {
                        if attempt < self.config.max_retries {
                            attempt += 1;
                            tokio::time::sleep(retry_delay(attempt)).await;
                            continue;
                        }

                        return Err(ProviderLookupError::rate_limited(
                            format!("QRZ XML request exceeded rate limits (HTTP {status})."),
                            debug_http_exchanges,
                        ));
                    }

                    if status.is_server_error() && attempt < self.config.max_retries {
                        attempt += 1;
                        tokio::time::sleep(retry_delay(attempt)).await;
                        continue;
                    }

                    return Err(ProviderLookupError::transport(
                        format!("QRZ XML request failed with HTTP status {status}."),
                        debug_http_exchanges,
                    ));
                }
                Err(error) => {
                    let exchange = DebugHttpExchange {
                        provider_name: "QRZ XML".to_string(),
                        operation: operation.to_string(),
                        started_at_utc: Some(started_at_utc),
                        duration_ms: duration_to_millis_u32(duration_start.elapsed()),
                        attempt: attempt + 1,
                        method,
                        url,
                        request_headers,
                        request_body: None,
                        response_status_code: None,
                        response_headers: Vec::new(),
                        response_body: None,
                        error_message: Some(format!("QRZ XML request failed: {error}")),
                    };
                    debug_http_exchanges.push(exchange);

                    if is_retryable_transport_error(&error) && attempt < self.config.max_retries {
                        attempt += 1;
                        tokio::time::sleep(retry_delay(attempt)).await;
                        continue;
                    }

                    return Err(ProviderLookupError::transport(
                        format!("QRZ XML request failed: {error}"),
                        debug_http_exchanges,
                    ));
                }
            }
        }
    }
}

#[tonic::async_trait]
impl CallsignProvider for QrzXmlProvider {
    async fn lookup_callsign(&self, callsign: &str) -> Result<ProviderLookup, ProviderLookupError> {
        let normalized_callsign = normalize_callsign(callsign);
        let mut session_retry_attempted = false;
        let mut debug_http_exchanges = Vec::new();

        loop {
            let (session_key, session_exchanges) =
                self.ensure_session_key().await.map_err(|error| {
                    error.with_prior_debug_http_exchanges(debug_http_exchanges.clone())
                })?;
            debug_http_exchanges.extend(session_exchanges);
            let (response, lookup_exchanges) = self
                .request_database(
                    &[
                        ("s", session_key),
                        ("callsign", normalized_callsign.clone()),
                    ],
                    "callsign_lookup",
                )
                .await
                .map_err(|error| {
                    error.with_prior_debug_http_exchanges(debug_http_exchanges.clone())
                })?;
            debug_http_exchanges.extend(lookup_exchanges);

            let session = response.session.ok_or_else(|| {
                ProviderLookupError::parse(
                    "QRZ lookup response did not include a <Session> element.".to_string(),
                    debug_http_exchanges.clone(),
                )
            })?;

            if let Some(key) = session.key.as_deref() {
                self.store_session_key(key).await;
            } else {
                self.clear_session_key().await;
            }

            if let Some(error) = session.error.as_deref() {
                if is_not_found_error(error) && session.key.is_some() {
                    return Ok(ProviderLookup::not_found(debug_http_exchanges));
                }

                if session.key.is_none() && !session_retry_attempted {
                    session_retry_attempted = true;
                    self.clear_session_key().await;
                    continue;
                }

                if is_auth_error(error) || is_connection_refused_error(error) {
                    return Err(ProviderLookupError::authentication(
                        error.to_string(),
                        debug_http_exchanges,
                    ));
                }

                return Err(ProviderLookupError::session(
                    error.to_string(),
                    debug_http_exchanges,
                ));
            }

            let callsign_record = response.callsign.ok_or_else(|| {
                ProviderLookupError::parse(
                    "QRZ lookup response omitted <Callsign> without a session error.".to_string(),
                    debug_http_exchanges.clone(),
                )
            })?;

            return Ok(ProviderLookup::found(
                map_callsign_record(&normalized_callsign, &callsign_record),
                debug_http_exchanges,
            ));
        }
    }
}

#[derive(Debug)]
struct CapturedQrzResponse {
    body: String,
    debug_http_exchanges: Vec<DebugHttpExchange>,
}

#[derive(Debug, Deserialize)]
struct QrzDatabase {
    #[serde(rename = "Session")]
    session: Option<QrzSession>,
    #[serde(rename = "Callsign")]
    callsign: Option<QrzCallsign>,
}

#[derive(Debug, Deserialize)]
struct QrzSession {
    #[serde(rename = "Key")]
    key: Option<String>,
    #[serde(rename = "Error")]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QrzCallsign {
    #[serde(rename = "call")]
    call: Option<String>,
    #[serde(rename = "xref")]
    cross_ref: Option<String>,
    #[serde(rename = "aliases")]
    aliases: Option<String>,
    #[serde(rename = "p_call")]
    previous_call: Option<String>,
    #[serde(rename = "dxcc")]
    dxcc: Option<String>,
    #[serde(rename = "fname")]
    first_name: Option<String>,
    #[serde(rename = "name")]
    last_name: Option<String>,
    #[serde(rename = "nickname")]
    nickname: Option<String>,
    #[serde(rename = "name_fmt")]
    formatted_name: Option<String>,
    #[serde(rename = "attn")]
    attention: Option<String>,
    #[serde(rename = "addr1")]
    addr1: Option<String>,
    #[serde(rename = "addr2")]
    addr2: Option<String>,
    #[serde(rename = "state")]
    state: Option<String>,
    #[serde(rename = "zip")]
    zip: Option<String>,
    #[serde(rename = "country")]
    country: Option<String>,
    #[serde(rename = "ccode")]
    country_code: Option<String>,
    #[serde(rename = "lat")]
    latitude: Option<String>,
    #[serde(rename = "lon")]
    longitude: Option<String>,
    #[serde(rename = "grid")]
    grid_square: Option<String>,
    #[serde(rename = "county")]
    county: Option<String>,
    #[serde(rename = "fips")]
    fips: Option<String>,
    #[serde(rename = "geoloc")]
    geo_source: Option<String>,
    #[serde(rename = "class")]
    license_class: Option<String>,
    #[serde(rename = "efdate")]
    effective_date: Option<String>,
    #[serde(rename = "expdate")]
    expiration_date: Option<String>,
    #[serde(rename = "codes")]
    license_codes: Option<String>,
    #[serde(rename = "email")]
    email: Option<String>,
    #[serde(rename = "url")]
    web_url: Option<String>,
    #[serde(rename = "qslmgr")]
    qsl_manager: Option<String>,
    #[serde(rename = "eqsl")]
    eqsl: Option<String>,
    #[serde(rename = "lotw")]
    lotw: Option<String>,
    #[serde(rename = "mqsl")]
    paper_qsl: Option<String>,
    #[serde(rename = "cqzone")]
    cq_zone: Option<String>,
    #[serde(rename = "ituzone")]
    itu_zone: Option<String>,
    #[serde(rename = "iota")]
    iota: Option<String>,
    #[serde(rename = "land")]
    dxcc_country_name: Option<String>,
    #[serde(rename = "continent")]
    continent: Option<String>,
    #[serde(rename = "born")]
    birth_year: Option<String>,
    #[serde(rename = "serial")]
    qrz_serial: Option<String>,
    #[serde(rename = "moddate")]
    last_modified: Option<String>,
    #[serde(rename = "bio")]
    bio: Option<String>,
    #[serde(rename = "image")]
    image_url: Option<String>,
    #[serde(rename = "MSA")]
    msa: Option<String>,
    #[serde(rename = "AreaCode")]
    area_code: Option<String>,
    #[serde(rename = "TimeZone")]
    time_zone: Option<String>,
    #[serde(rename = "GMTOffset")]
    gmt_offset: Option<String>,
    #[serde(rename = "DST")]
    dst_observed: Option<String>,
    #[serde(rename = "u_views")]
    profile_views: Option<String>,
}

#[allow(clippy::too_many_lines)]
fn map_callsign_record(queried_callsign: &str, qrz: &QrzCallsign) -> CallsignRecord {
    let callsign = qrz
        .call
        .as_deref()
        .map_or_else(|| queried_callsign.to_string(), normalize_callsign);
    let cross_ref = qrz
        .cross_ref
        .as_deref()
        .map_or_else(|| queried_callsign.to_string(), normalize_callsign);

    let mut record = CallsignRecord {
        callsign,
        cross_ref,
        aliases: parse_aliases(qrz.aliases.as_deref()),
        previous_call: optional_string(qrz.previous_call.as_deref()).unwrap_or_default(),
        dxcc_entity_id: parse_u32(qrz.dxcc.as_deref()).unwrap_or_default(),
        first_name: optional_string(qrz.first_name.as_deref()).unwrap_or_default(),
        last_name: optional_string(qrz.last_name.as_deref()).unwrap_or_default(),
        nickname: optional_string(qrz.nickname.as_deref()),
        formatted_name: optional_string(qrz.formatted_name.as_deref()),
        attention: optional_string(qrz.attention.as_deref()),
        addr1: optional_string(qrz.addr1.as_deref()),
        addr2: optional_string(qrz.addr2.as_deref()),
        state: optional_string(qrz.state.as_deref()),
        zip: optional_string(qrz.zip.as_deref()),
        country: optional_string(qrz.country.as_deref()),
        country_code: parse_u32(qrz.country_code.as_deref()),
        latitude: parse_f64(qrz.latitude.as_deref()),
        longitude: parse_f64(qrz.longitude.as_deref()),
        grid_square: optional_string(qrz.grid_square.as_deref()),
        county: optional_string(qrz.county.as_deref()),
        fips: optional_string(qrz.fips.as_deref()),
        geo_source: map_geo_source(qrz.geo_source.as_deref()),
        license_class: optional_string(qrz.license_class.as_deref()),
        effective_date: parse_date_timestamp(qrz.effective_date.as_deref()),
        expiration_date: parse_date_timestamp(qrz.expiration_date.as_deref()),
        license_codes: optional_string(qrz.license_codes.as_deref()),
        email: optional_string(qrz.email.as_deref()),
        web_url: optional_string(qrz.web_url.as_deref()),
        qsl_manager: optional_string(qrz.qsl_manager.as_deref()),
        eqsl: map_qsl_preference(qrz.eqsl.as_deref()),
        lotw: map_qsl_preference(qrz.lotw.as_deref()),
        paper_qsl: map_qsl_preference(qrz.paper_qsl.as_deref()),
        cq_zone: parse_u32(qrz.cq_zone.as_deref()),
        itu_zone: parse_u32(qrz.itu_zone.as_deref()),
        iota: optional_string(qrz.iota.as_deref()),
        dxcc_country_name: optional_string(qrz.dxcc_country_name.as_deref()),
        dxcc_continent: optional_string(qrz.continent.as_deref()),
        birth_year: parse_u32(qrz.birth_year.as_deref()),
        qrz_serial: parse_u64(qrz.qrz_serial.as_deref()),
        last_modified: parse_datetime_timestamp(qrz.last_modified.as_deref()),
        bio_length: parse_bio_length(qrz.bio.as_deref()),
        image_url: optional_string(qrz.image_url.as_deref()),
        msa: optional_string(qrz.msa.as_deref()),
        area_code: optional_string(qrz.area_code.as_deref()),
        time_zone: optional_string(qrz.time_zone.as_deref()),
        gmt_offset: parse_gmt_offset(qrz.gmt_offset.as_deref()),
        dst_observed: parse_yes_no(qrz.dst_observed.as_deref()),
        profile_views: parse_u32(qrz.profile_views.as_deref()),
        base_callsign: None,
        modifier_text: None,
        modifier_kind: None,
        prefix_override_callsign: None,
        operating_entity_hint: None,
        callsign_ambiguity: None,
    };

    // Derive missing CQ zone from location data (state, lat/lon, grid square)
    // for multi-zone countries like the USA, Canada, and Australia.
    crate::adif::enrich_zones_from_location(&mut record);

    // Fill remaining gaps (continent, country name, ITU zone, CQ zone for
    // entities without location-based rules) from the DXCC entity table.
    crate::adif::enrich_callsign_record_from_dxcc(&mut record);

    record
}

fn required_value<F>(name: &'static str, get_value: &mut F) -> Result<String, QrzXmlConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    match get_value(name) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(QrzXmlConfigError::Missing { name }),
    }
}

fn optional_value<F>(name: &'static str, get_value: &mut F) -> Option<String>
where
    F: FnMut(&'static str) -> Option<String>,
{
    get_value(name).and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn optional_value_u64<F>(
    name: &'static str,
    default: u64,
    get_value: &mut F,
) -> Result<u64, QrzXmlConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    match get_value(name) {
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| QrzXmlConfigError::InvalidInteger { name, value: raw }),
        None => Ok(default),
    }
}

fn optional_value_u32<F>(
    name: &'static str,
    default: u32,
    get_value: &mut F,
) -> Result<u32, QrzXmlConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    match get_value(name) {
        Some(raw) => raw
            .trim()
            .parse::<u32>()
            .map_err(|_| QrzXmlConfigError::InvalidInteger { name, value: raw }),
        None => Ok(default),
    }
}

fn optional_value_bool<F>(
    name: &'static str,
    default: bool,
    get_value: &mut F,
) -> Result<bool, QrzXmlConfigError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    match get_value(name) {
        Some(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "y" | "on" => Ok(true),
                "0" | "false" | "no" | "n" | "off" => Ok(false),
                _ => Err(QrzXmlConfigError::InvalidBoolean { name, value: raw }),
            }
        }
        None => Ok(default),
    }
}

fn normalize_base_url(base_url: String) -> String {
    if base_url.ends_with('/') {
        base_url
    } else {
        format!("{base_url}/")
    }
}

fn retry_delay(attempt: u32) -> Duration {
    let shift = attempt.min(6);
    Duration::from_millis(RETRY_BASE_DELAY_MILLIS.saturating_mul(1_u64 << shift))
}

fn duration_to_millis_u32(duration: Duration) -> u32 {
    match u32::try_from(duration.as_millis()) {
        Ok(value) => value,
        Err(_) => u32::MAX,
    }
}

fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
}

fn is_not_found_error(error: &str) -> bool {
    error.to_ascii_lowercase().contains("not found")
}

fn is_connection_refused_error(error: &str) -> bool {
    error.trim().eq_ignore_ascii_case("connection refused")
}

fn is_auth_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("password")
        || lowered.contains("username")
        || lowered.contains("login")
        || lowered.contains("authorization")
}

fn redact_request_url(request: &Request) -> String {
    let mut base_url = request.url().clone();
    let masked_query = request
        .url()
        .query_pairs()
        .map(|(name, value)| format!("{name}={}", mask_capture_value(&name, &value)))
        .collect::<Vec<_>>()
        .join("&");
    base_url.set_query(None);
    if masked_query.is_empty() {
        base_url.to_string()
    } else {
        format!("{base_url}?{masked_query}")
    }
}

fn capture_headers(headers: &HeaderMap) -> Vec<DebugHttpHeader> {
    let mut captured = headers
        .iter()
        .map(|(name, value)| DebugHttpHeader {
            name: name.as_str().to_string(),
            value: mask_header_value(name.as_str(), value.to_str().unwrap_or("<non-utf8>")),
        })
        .collect::<Vec<_>>();
    captured.sort_by(|left, right| left.name.cmp(&right.name));
    captured
}

fn mask_header_value(name: &str, value: &str) -> String {
    if is_sensitive_header_name(name) {
        "<redacted>".to_string()
    } else {
        value.to_string()
    }
}

fn is_sensitive_header_name(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "authorization" | "cookie" | "set-cookie" | "proxy-authorization" | "x-api-key"
    )
}

fn redact_qrz_xml_response(response_body: &str) -> String {
    redact_xml_tag_contents(response_body, "Key")
}

fn redact_xml_tag_contents(xml: &str, tag_name: &str) -> String {
    let open_tag = format!("<{tag_name}>");
    let close_tag = format!("</{tag_name}>");
    let mut remaining = xml;
    let mut redacted = String::with_capacity(xml.len());

    while let Some(open_index) = remaining.find(&open_tag) {
        let (before_open, after_before_open) = remaining.split_at(open_index);
        redacted.push_str(before_open);
        redacted.push_str(&open_tag);

        let after_open = &after_before_open[open_tag.len()..];
        let Some(close_index) = after_open.find(&close_tag) else {
            redacted.push_str("<redacted>");
            return redacted;
        };

        redacted.push_str("<redacted>");
        redacted.push_str(&after_open[close_index..close_index + close_tag.len()]);
        remaining = &after_open[close_index + close_tag.len()..];
    }

    redacted.push_str(remaining);
    redacted
}

fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
    let Ok(duration) = time.duration_since(UNIX_EPOCH) else {
        return Timestamp::default();
    };

    Timestamp {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(duration.subsec_nanos()).unwrap_or_default(),
    }
}

fn mask_capture_value(name: &str, value: &str) -> String {
    if is_sensitive_query_key(name) {
        "<redacted>".to_string()
    } else {
        value.to_string()
    }
}

fn render_capture_query(name: &str, value: &str) -> String {
    let diagnostics = value_diagnostics(value);
    let payload = if is_sensitive_query_key(name) {
        "<redacted>".to_string()
    } else {
        format!("{value:?}")
    };
    format!(
        "{name}={payload} (chars={}, bytes={}, leading_ws={}, trailing_ws={}, starts_with_quote={}, ends_with_quote={})",
        diagnostics.char_count,
        diagnostics.byte_count,
        diagnostics.leading_whitespace,
        diagnostics.trailing_whitespace,
        diagnostics.starts_with_quote,
        diagnostics.ends_with_quote
    )
}

fn is_sensitive_query_key(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "password" | "pwd" | "s" | "key" | "api_key" | "apikey" | "token"
    )
}

fn value_diagnostics(value: &str) -> ValueDiagnostics {
    let char_count = value.chars().count();
    let byte_count = value.len();
    let leading_whitespace = value.chars().take_while(|ch| ch.is_whitespace()).count();
    let trailing_whitespace = value
        .chars()
        .rev()
        .take_while(|ch| ch.is_whitespace())
        .count();
    let starts_with_quote = matches!(value.chars().next(), Some('"' | '\''));
    let ends_with_quote = matches!(value.chars().next_back(), Some('"' | '\''));

    ValueDiagnostics {
        char_count,
        byte_count,
        leading_whitespace,
        trailing_whitespace,
        starts_with_quote,
        ends_with_quote,
    }
}

#[derive(Debug, Clone, Copy)]
struct ValueDiagnostics {
    char_count: usize,
    byte_count: usize,
    leading_whitespace: usize,
    trailing_whitespace: usize,
    starts_with_quote: bool,
    ends_with_quote: bool,
}

fn optional_string(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_aliases(value: Option<&str>) -> Vec<String> {
    value.map_or_else(Vec::new, |raw| {
        raw.split(',')
            .filter_map(|alias| optional_string(Some(alias)))
            .collect()
    })
}

fn parse_u32(value: Option<&str>) -> Option<u32> {
    value?.trim().parse::<u32>().ok()
}

fn parse_u64(value: Option<&str>) -> Option<u64> {
    value?.trim().parse::<u64>().ok()
}

fn parse_f64(value: Option<&str>) -> Option<f64> {
    value?.trim().parse::<f64>().ok()
}

fn parse_yes_no(value: Option<&str>) -> Option<bool> {
    match value?.trim().to_ascii_uppercase().as_str() {
        "Y" | "YES" | "1" => Some(true),
        "N" | "NO" | "0" => Some(false),
        _ => None,
    }
}

fn parse_date_timestamp(value: Option<&str>) -> Option<Timestamp> {
    let date = crate::utc::parse_date(value?.trim()).ok()?;
    Some(crate::utc::to_timestamp(date.midnight().assume_utc()))
}

fn parse_datetime_timestamp(value: Option<&str>) -> Option<Timestamp> {
    let date_time = time::PrimitiveDateTime::parse(
        value?.trim(),
        time::macros::format_description!("[year]-[month padding:none]-[day padding:none] [hour padding:none]:[minute padding:none]:[second padding:none]"),
    )
    .ok()?;
    Some(crate::utc::to_timestamp(date_time.assume_utc()))
}

fn parse_bio_length(value: Option<&str>) -> Option<u32> {
    let raw = value?.trim();
    let size_part = raw.split_once('/').map_or(raw, |(size, _)| size);
    size_part.parse::<u32>().ok()
}

fn parse_gmt_offset(value: Option<&str>) -> Option<f64> {
    let raw = value?.trim();
    if raw.is_empty() {
        return None;
    }

    let (sign, digits) = if let Some(stripped) = raw.strip_prefix('-') {
        (-1_f64, stripped)
    } else if let Some(stripped) = raw.strip_prefix('+') {
        (1_f64, stripped)
    } else {
        (1_f64, raw)
    };

    if digits.len() == 4 && digits.chars().all(|character| character.is_ascii_digit()) {
        let hours = digits[0..2].parse::<f64>().ok()?;
        let minutes = digits[2..4].parse::<f64>().ok()?;
        return Some(sign * (hours + (minutes / 60.0)));
    }

    raw.parse::<f64>().ok()
}

fn map_geo_source(value: Option<&str>) -> i32 {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "user" => GeoSource::User as i32,
        "geocode" => GeoSource::Geocode as i32,
        "grid" => GeoSource::Grid as i32,
        "zip" => GeoSource::Zip as i32,
        "state" => GeoSource::State as i32,
        "dxcc" => GeoSource::Dxcc as i32,
        "none" => GeoSource::None as i32,
        _ => GeoSource::Unspecified as i32,
    }
}

fn map_qsl_preference(value: Option<&str>) -> i32 {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "1" | "Y" => QslPreference::Yes as i32,
        "0" | "N" => QslPreference::No as i32,
        _ => QslPreference::Unknown as i32,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{
        sync::{Arc, Mutex as StdMutex, OnceLock},
        time::Duration,
    };

    use super::*;
    use crate::lookup::provider::ProviderLookupOutcome;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    const LOGIN_SUCCESS_XML: &str = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Key>session-key</Key>
  </Session>
</QRZDatabase>
"#;

    const LOGIN_AUTH_FAILURE_XML: &str = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Error>Password incorrect</Error>
  </Session>
</QRZDatabase>
"#;

    const LOOKUP_NOT_FOUND_XML: &str = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Key>session-key</Key>
    <Error>Not found: w1aw</Error>
  </Session>
</QRZDatabase>
"#;

    const SESSION_TIMEOUT_XML: &str = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Error>Session Timeout</Error>
  </Session>
</QRZDatabase>
"#;

    const MALFORMED_LOOKUP_XML: &str = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Key>session-key
"#;

    const FOUND_XML: &str = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Key>session-key</Key>
  </Session>
  <Callsign>
    <call>w1aw</call>
    <xref>w1aw</xref>
    <aliases>N1AW,K1AW</aliases>
    <dxcc>291</dxcc>
    <fname>Hiram</fname>
    <name>Maxim</name>
    <addr2>Newington</addr2>
    <country>United States</country>
    <ccode>291</ccode>
    <lat>41.7148</lat>
    <lon>-72.7272</lon>
    <grid>FN31pr</grid>
    <geoloc>user</geoloc>
    <class>E</class>
    <efdate>2020-01-01</efdate>
    <expdate>2030-01-01</expdate>
    <eqsl>1</eqsl>
    <lotw>Y</lotw>
    <mqsl>0</mqsl>
    <cqzone>5</cqzone>
    <ituzone>8</ituzone>
    <land>United States</land>
    <continent>NA</continent>
    <serial>1234</serial>
    <moddate>2025-01-01 12:34:56</moddate>
    <bio>2048/2024-06-10</bio>
    <image>https://example.com/w1aw.jpg</image>
    <MSA>1234</MSA>
    <AreaCode>860</AreaCode>
    <TimeZone>Eastern</TimeZone>
    <GMTOffset>-5</GMTOffset>
    <DST>Y</DST>
    <u_views>77</u_views>
  </Callsign>
</QRZDatabase>
"#;

    fn test_config(base_url: String) -> QrzXmlConfig {
        QrzXmlConfig {
            base_url,
            username: "KC7AVA".to_string(),
            password: "super-secret-password".to_string(),
            user_agent: "QsoRipper/0.1.0 (KC7AVA)".to_string(),
            http_timeout: Duration::from_secs(2),
            max_retries: 0,
            capture_only: false,
        }
    }

    async fn spawn_qrz_server(responses: &[&str]) -> (String, Arc<StdMutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let recorded_requests = Arc::new(StdMutex::new(Vec::new()));
        let recorded_requests_clone = Arc::clone(&recorded_requests);
        let responses = responses
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>();

        tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let request = read_http_request(&mut socket).await;
                recorded_requests_clone
                    .lock()
                    .expect("recorded requests")
                    .push(request);
                write_http_response(&mut socket, &response).await;
            }
        });

        (format!("http://{address}/xml/current/"), recorded_requests)
    }

    async fn read_http_request(socket: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = socket.read(&mut buffer).await.expect("read request");
            if read == 0 {
                break;
            }

            bytes.extend_from_slice(buffer.get(..read).expect("buffer slice"));
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        String::from_utf8(bytes).expect("request utf8")
    }

    async fn write_http_response(socket: &mut TcpStream, response_body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    }

    #[test]
    fn qrz_callsign_payload_maps_to_proto_record() {
        let parsed: QrzDatabase = quick_xml::de::from_str(FOUND_XML).expect("parse");
        let callsign = parsed.callsign.as_ref().expect("callsign node");
        let mapped = map_callsign_record("W1AW", callsign);

        assert_eq!(mapped.callsign, "W1AW");
        assert_eq!(mapped.cross_ref, "W1AW");
        assert_eq!(mapped.aliases, vec!["N1AW", "K1AW"]);
        assert_eq!(mapped.dxcc_entity_id, 291);
        assert_eq!(mapped.first_name, "Hiram");
        assert_eq!(mapped.last_name, "Maxim");
        assert_eq!(mapped.grid_square.as_deref(), Some("FN31pr"));
        assert_eq!(mapped.geo_source, GeoSource::User as i32);
        assert_eq!(mapped.eqsl, QslPreference::Yes as i32);
        assert_eq!(mapped.paper_qsl, QslPreference::No as i32);
        assert_eq!(mapped.profile_views, Some(77));
        assert_eq!(mapped.bio_length, Some(2048));
        assert_eq!(mapped.cq_zone, Some(5));
        assert_eq!(mapped.itu_zone, Some(8));
        assert_eq!(mapped.dxcc_country_name.as_deref(), Some("United States"));
        assert_eq!(mapped.dxcc_continent.as_deref(), Some("NA"));
    }

    #[test]
    fn dxcc_fallback_fills_missing_continent_and_zones() {
        // Simulates a QRZ response that omits <cqzone>, <ituzone>, and <continent>
        // (common for US callsigns like AE7XI).
        let xml_without_zones = r#"
<?xml version="1.0"?>
<QRZDatabase version="1.34">
  <Session>
    <Key>session-key</Key>
  </Session>
  <Callsign>
    <call>AE7XI</call>
    <dxcc>291</dxcc>
    <fname>MIKE A</fname>
    <name>TREIT, JR</name>
    <country>United States</country>
    <land>United States</land>
    <grid>CN87xr</grid>
  </Callsign>
</QRZDatabase>
"#;
        let parsed: QrzDatabase = quick_xml::de::from_str(xml_without_zones).expect("parse");
        let callsign = parsed.callsign.as_ref().expect("callsign node");
        let mapped = map_callsign_record("AE7XI", callsign);

        // CN87xr is in western Washington → CQ zone 3 via grid-square derivation.
        // Before location-based enrichment this would have fallen through to the
        // DXCC-291 default of zone 5.
        assert_eq!(mapped.dxcc_continent.as_deref(), Some("NA"));
        assert_eq!(mapped.cq_zone, Some(3));
        assert_eq!(mapped.itu_zone, Some(6));
    }

    #[test]
    fn dxcc_fallback_preserves_qrz_provided_values() {
        // When QRZ provides all fields, the fallback must not overwrite them.
        let parsed: QrzDatabase = quick_xml::de::from_str(FOUND_XML).expect("parse");
        let callsign = parsed.callsign.as_ref().expect("callsign node");
        let mapped = map_callsign_record("W1AW", callsign);

        // QRZ provided cqzone=5, ituzone=8, continent=NA — these must be preserved.
        assert_eq!(mapped.cq_zone, Some(5));
        assert_eq!(mapped.itu_zone, Some(8));
        assert_eq!(mapped.dxcc_continent.as_deref(), Some("NA"));
    }

    #[test]
    fn not_found_error_detection_is_case_insensitive() {
        assert!(is_not_found_error("Not found: W1AW"));
        assert!(is_not_found_error("not FOUND: xx0xx"));
        assert!(!is_not_found_error("Session timeout"));
    }

    #[test]
    fn parse_gmt_offset_supports_hhmm_format() {
        let offset = parse_gmt_offset(Some("0545")).expect("offset");
        assert!((offset - 5.75).abs() < 0.000_01);
    }

    #[tokio::test]
    async fn lookup_callsign_logs_in_then_queries_by_session_key() {
        let (base_url, recorded_requests) = spawn_qrz_server(&[LOGIN_SUCCESS_XML, FOUND_XML]).await;
        let provider = QrzXmlProvider::new(test_config(base_url)).expect("provider");

        let result = provider.lookup_callsign("w1aw").await.expect("lookup");

        let ProviderLookup {
            outcome,
            debug_http_exchanges,
        } = result;
        let record = match outcome {
            ProviderLookupOutcome::Found(record) => Some(record),
            ProviderLookupOutcome::NotFound => None,
        }
        .expect("expected found result");
        assert_eq!(record.callsign, "W1AW");
        assert_eq!(debug_http_exchanges.len(), 2);
        let first_exchange = debug_http_exchanges.first().expect("login exchange");
        let second_exchange = debug_http_exchanges.get(1).expect("lookup exchange");
        assert_eq!(first_exchange.operation, "login");
        assert_eq!(second_exchange.operation, "callsign_lookup");
        assert_eq!(
            first_exchange.response_body.as_deref(),
            Some(
                "\n<?xml version=\"1.0\"?>\n<QRZDatabase version=\"1.34\">\n  <Session>\n    <Key><redacted></Key>\n  </Session>\n</QRZDatabase>\n"
            )
        );
        assert!(second_exchange.url.contains("s=<redacted>"));
        assert!(!second_exchange
            .response_body
            .as_deref()
            .unwrap_or_default()
            .contains("session-key"));

        let requests = recorded_requests.lock().expect("requests");
        assert_eq!(2, requests.len());
        let login_request = requests.first().expect("login request");
        let lookup_request = requests.get(1).expect("lookup request");
        assert!(login_request.contains("username=KC7AVA"));
        assert!(login_request.contains("password=super-secret-password"));
        assert!(login_request.contains("agent=QsoRipper"));
        assert!(lookup_request.contains("s=session-key"));
        assert!(lookup_request.contains("callsign=W1AW"));
    }

    #[tokio::test]
    async fn lookup_callsign_retries_after_session_timeout_once() {
        let (base_url, recorded_requests) = spawn_qrz_server(&[
            LOGIN_SUCCESS_XML,
            SESSION_TIMEOUT_XML,
            LOGIN_SUCCESS_XML,
            FOUND_XML,
        ])
        .await;
        let provider = QrzXmlProvider::new(test_config(base_url)).expect("provider");

        let result = provider.lookup_callsign("w1aw").await.expect("lookup");

        assert!(matches!(result.outcome, ProviderLookupOutcome::Found(_)));
        assert_eq!(result.debug_http_exchanges.len(), 4);
        assert_eq!(4, recorded_requests.lock().expect("requests").len());
    }

    #[tokio::test]
    async fn lookup_callsign_returns_not_found_when_qrz_keeps_session_key() {
        let (base_url, _) = spawn_qrz_server(&[LOGIN_SUCCESS_XML, LOOKUP_NOT_FOUND_XML]).await;
        let provider = QrzXmlProvider::new(test_config(base_url)).expect("provider");

        let result = provider.lookup_callsign("w1aw").await.expect("lookup");

        assert!(matches!(result.outcome, ProviderLookupOutcome::NotFound));
        assert_eq!(result.debug_http_exchanges.len(), 2);
    }

    #[tokio::test]
    async fn lookup_callsign_surfaces_login_authentication_failures() {
        let (base_url, _) = spawn_qrz_server(&[LOGIN_AUTH_FAILURE_XML]).await;
        let provider = QrzXmlProvider::new(test_config(base_url)).expect("provider");

        let error = provider
            .lookup_callsign("w1aw")
            .await
            .expect_err("auth error");

        assert_eq!(
            "Provider authentication error: QRZ login failed: Password incorrect",
            error.to_string()
        );
    }

    #[tokio::test]
    async fn lookup_failure_after_login_preserves_login_and_lookup_captures() {
        let (base_url, _) = spawn_qrz_server(&[LOGIN_SUCCESS_XML, MALFORMED_LOOKUP_XML]).await;
        let provider = QrzXmlProvider::new(test_config(base_url)).expect("provider");

        let error = provider
            .lookup_callsign("w1aw")
            .await
            .expect_err("parse error");

        assert_eq!(error.debug_http_exchanges().len(), 2);
        let first_exchange = error
            .debug_http_exchanges()
            .first()
            .expect("login exchange");
        let second_exchange = error
            .debug_http_exchanges()
            .get(1)
            .expect("lookup exchange");
        assert_eq!(first_exchange.operation, "login");
        assert_eq!(second_exchange.operation, "callsign_lookup");
        assert!(!second_exchange
            .response_body
            .as_deref()
            .unwrap_or_default()
            .contains("session-key"));
    }

    #[tokio::test]
    async fn capture_only_mode_returns_redacted_request_diagnostics() {
        let mut config = test_config("https://xmldata.qrz.com/xml/current/".to_string());
        config.capture_only = true;
        let provider = QrzXmlProvider::new(config).expect("provider");

        let error = provider
            .lookup_callsign("w1aw")
            .await
            .expect_err("capture error");
        let message = error.to_string();

        assert!(message.contains("request not sent"));
        assert!(message.contains("username=KC7AVA"));
        assert!(message.contains("password=<redacted>"));
        assert!(!message.contains("super-secret-password"));
        assert!(message.contains("starts_with_quote=false"));
        assert_eq!(error.debug_http_exchanges().len(), 1);
        let login_exchange = error
            .debug_http_exchanges()
            .first()
            .expect("login exchange");
        assert_eq!(login_exchange.operation, "login");
        assert!(login_exchange.url.contains("password=<redacted>"));
    }

    #[test]
    fn capture_rendering_redacts_sensitive_values() {
        let rendered = render_capture_query("password", "\"secret\"");
        assert!(rendered.contains("password=<redacted>"));
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("starts_with_quote=true"));
        assert!(rendered.contains("ends_with_quote=true"));
    }

    #[test]
    fn response_redaction_masks_unterminated_session_key() {
        let redacted = redact_qrz_xml_response(MALFORMED_LOOKUP_XML);
        assert!(!redacted.contains("session-key"));
        assert!(redacted.contains("<Key><redacted>"));
    }

    #[test]
    fn qrz_config_supports_capture_only_flag_from_env() {
        let _guard = env_lock().lock().expect("env lock");
        let saved_username = std::env::var("QSORIPPER_QRZ_XML_USERNAME").ok();
        let saved_password = std::env::var("QSORIPPER_QRZ_XML_PASSWORD").ok();
        let saved_agent = std::env::var("QSORIPPER_QRZ_USER_AGENT").ok();
        let saved_base_url = std::env::var("QSORIPPER_QRZ_XML_BASE_URL").ok();
        let saved_capture_only = std::env::var(QRZ_XML_CAPTURE_ONLY_ENV_VAR).ok();

        std::env::set_var("QSORIPPER_QRZ_XML_USERNAME", "KC7AVA");
        std::env::set_var("QSORIPPER_QRZ_XML_PASSWORD", "super-secret-password");
        std::env::set_var("QSORIPPER_QRZ_USER_AGENT", "QsoRipper/0.1.0 (KC7AVA)");
        std::env::set_var(
            "QSORIPPER_QRZ_XML_BASE_URL",
            "https://xmldata.qrz.com/xml/current",
        );
        std::env::set_var(QRZ_XML_CAPTURE_ONLY_ENV_VAR, "true");

        let config = QrzXmlConfig::from_env().expect("config");

        assert_eq!("https://xmldata.qrz.com/xml/current/", config.base_url);
        assert!(config.capture_only);

        restore_env("QSORIPPER_QRZ_XML_USERNAME", saved_username);
        restore_env("QSORIPPER_QRZ_XML_PASSWORD", saved_password);
        restore_env("QSORIPPER_QRZ_USER_AGENT", saved_agent);
        restore_env("QSORIPPER_QRZ_XML_BASE_URL", saved_base_url);
        restore_env(QRZ_XML_CAPTURE_ONLY_ENV_VAR, saved_capture_only);
    }

    #[test]
    fn qrz_config_rejects_invalid_capture_only_value() {
        let _guard = env_lock().lock().expect("env lock");
        let saved_username = std::env::var("QSORIPPER_QRZ_XML_USERNAME").ok();
        let saved_password = std::env::var("QSORIPPER_QRZ_XML_PASSWORD").ok();
        let saved_agent = std::env::var("QSORIPPER_QRZ_USER_AGENT").ok();
        let saved_capture_only = std::env::var(QRZ_XML_CAPTURE_ONLY_ENV_VAR).ok();

        std::env::set_var("QSORIPPER_QRZ_XML_USERNAME", "KC7AVA");
        std::env::set_var("QSORIPPER_QRZ_XML_PASSWORD", "super-secret-password");
        std::env::set_var("QSORIPPER_QRZ_USER_AGENT", "QsoRipper/0.1.0 (KC7AVA)");
        std::env::set_var(QRZ_XML_CAPTURE_ONLY_ENV_VAR, "sometimes");

        let error = QrzXmlConfig::from_env().expect_err("invalid bool");

        assert_eq!(
            "Environment variable 'QSORIPPER_QRZ_XML_CAPTURE_ONLY' has invalid boolean value 'sometimes'.",
            error.to_string()
        );

        restore_env("QSORIPPER_QRZ_XML_USERNAME", saved_username);
        restore_env("QSORIPPER_QRZ_XML_PASSWORD", saved_password);
        restore_env("QSORIPPER_QRZ_USER_AGENT", saved_agent);
        restore_env(QRZ_XML_CAPTURE_ONLY_ENV_VAR, saved_capture_only);
    }

    #[test]
    fn sensitive_query_detection_catches_qrz_session_and_password_fields() {
        assert!(is_sensitive_query_key("password"));
        assert!(is_sensitive_query_key("s"));
        assert!(!is_sensitive_query_key("agent"));
        assert!(!is_sensitive_query_key("callsign"));
    }

    fn restore_env(name: &str, value: Option<String>) {
        if let Some(value) = value {
            std::env::set_var(name, value);
        } else {
            std::env::remove_var(name);
        }
    }

    fn env_lock() -> &'static StdMutex<()> {
        static ENV_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| StdMutex::new(()))
    }

    // ── Zone-from-location integration tests ────────────────────────────

    #[test]
    fn map_callsign_record_derives_zone_3_for_wa_state() {
        use crate::proto::qsoripper::domain::CallsignRecord;

        let mut record = CallsignRecord {
            dxcc_entity_id: 291,
            state: Some("WA".to_owned()),
            ..CallsignRecord::default()
        };

        // Simulate the enrichment cascade used in map_callsign_record.
        crate::adif::enrich_zones_from_location(&mut record);
        crate::adif::enrich_callsign_record_from_dxcc(&mut record);

        assert_eq!(
            record.cq_zone,
            Some(3),
            "WA should be CQ zone 3, not the DXCC-291 default of 5"
        );
    }

    #[test]
    fn map_callsign_record_preserves_existing_cq_zone() {
        use crate::proto::qsoripper::domain::CallsignRecord;

        let mut record = CallsignRecord {
            dxcc_entity_id: 291,
            state: Some("WA".to_owned()),
            cq_zone: Some(42),
            ..CallsignRecord::default()
        };

        crate::adif::enrich_zones_from_location(&mut record);
        crate::adif::enrich_callsign_record_from_dxcc(&mut record);

        assert_eq!(
            record.cq_zone,
            Some(42),
            "pre-existing zone must be preserved"
        );
    }

    #[test]
    fn map_callsign_record_derives_zone_from_grid_when_no_state() {
        use crate::proto::qsoripper::domain::CallsignRecord;

        let mut record = CallsignRecord {
            dxcc_entity_id: 291,
            grid_square: Some("CN87".to_owned()),
            ..CallsignRecord::default()
        };

        crate::adif::enrich_zones_from_location(&mut record);
        crate::adif::enrich_callsign_record_from_dxcc(&mut record);

        assert_eq!(
            record.cq_zone,
            Some(3),
            "CN87 grid is in western US → zone 3"
        );
    }
}
