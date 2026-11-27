use std::sync::{Arc, OnceLock};

const PACKAGE: &str = "com.treitforge.qsoripper";
const SERVICE: &str = "qrz";
#[cfg(target_os = "macos")]
const FULL_SERVICE: &str = "com.treitforge.qsoripper.qrz";

#[derive(Clone, Copy, Debug)]
pub(crate) enum QrzSecret {
    XmlPassword,
    LogbookApiKey,
}

impl QrzSecret {
    fn account(self) -> &'static str {
        match self {
            Self::XmlPassword => "xml-password",
            Self::LogbookApiKey => "logbook-api-key",
        }
    }
}

pub(crate) trait QrzSecretStore: Send + Sync {
    fn get(&self, secret: QrzSecret) -> Result<Option<String>, String>;
    fn set(&self, secret: QrzSecret, value: &str) -> Result<(), String>;
}

pub(crate) struct PlatformQrzSecretStore {
    store: OnceLock<Result<Arc<keyring_core::CredentialStore>, String>>,
}

impl PlatformQrzSecretStore {
    pub(crate) fn new() -> Self {
        Self {
            store: OnceLock::new(),
        }
    }

    fn entry(&self, secret: QrzSecret) -> Result<keyring_core::Entry, String> {
        let store = self
            .store
            .get_or_init(Self::platform_store)
            .as_ref()
            .map_err(Clone::clone)?;
        let account = secret.account();

        #[cfg(target_os = "windows")]
        let entry =
            keyring_core::Entry::new_in_store(store, &format!("{SERVICE}/{account}"), PACKAGE);

        #[cfg(target_os = "macos")]
        let entry = keyring_core::Entry::new_in_store(store, FULL_SERVICE, account);

        #[cfg(all(unix, not(target_os = "macos")))]
        let entry = keyring_core::Entry::new_in_store(store, SERVICE, account);

        #[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
        return Err("The platform credential store is not supported.".to_string());

        entry.map_err(|error| format!("Platform credential store is unavailable: {error}"))
    }

    fn platform_store() -> Result<Arc<keyring_core::CredentialStore>, String> {
        #[cfg(target_os = "windows")]
        let store = windows_native_keyring_store::Store::new();

        #[cfg(target_os = "macos")]
        let store = apple_native_keyring_store::keychain::Store::new();

        #[cfg(all(unix, not(target_os = "macos")))]
        let store = dbus_secret_service_keyring_store::Store::new();

        #[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
        return Err("The platform credential store is not supported.".to_string());

        store
            .map(|store| -> Arc<keyring_core::CredentialStore> { store })
            .map_err(|error| format!("Platform credential store is unavailable: {error}"))
    }
}

impl QrzSecretStore for PlatformQrzSecretStore {
    fn get(&self, secret: QrzSecret) -> Result<Option<String>, String> {
        match self.entry(secret)?.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring_core::Error::NoEntry) => Ok(None),
            Err(error) => Err(format!(
                "Failed to read the platform credential store: {error}"
            )),
        }
    }

    fn set(&self, secret: QrzSecret, value: &str) -> Result<(), String> {
        let entry = self.entry(secret)?;
        entry
            .set_password(value)
            .map_err(|error| format!("Failed to write the platform credential store: {error}"))?;

        #[cfg(all(unix, not(target_os = "macos")))]
        update_secret_service_schema(&entry)?;

        Ok(())
    }
}

#[cfg(any(all(unix, not(target_os = "macos")), test))]
fn update_secret_service_schema(entry: &keyring_core::Entry) -> Result<(), String> {
    entry
        .update_attributes(&std::collections::HashMap::from([("xdg:schema", PACKAGE)]))
        .map_err(|error| format!("Failed to set the platform credential store schema: {error}"))
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct MemoryQrzSecretStore {
    values: std::sync::RwLock<std::collections::HashMap<&'static str, String>>,
}

#[cfg(test)]
impl QrzSecretStore for MemoryQrzSecretStore {
    fn get(&self, secret: QrzSecret) -> Result<Option<String>, String> {
        let values = self
            .values
            .read()
            .map_err(|_| "Memory credential store read failed.".to_string())?;
        Ok(values.get(secret.account()).cloned())
    }

    fn set(&self, secret: QrzSecret, value: &str) -> Result<(), String> {
        let mut values = self
            .values
            .write()
            .map_err(|_| "Memory credential store write failed.".to_string())?;
        values.insert(secret.account(), value.to_string());
        Ok(())
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    reason = "Tests require isolated mock credentials"
)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use keyring_core::api::{CredentialApi, CredentialStoreApi};
    use keyring_core::{mock, Credential, CredentialStore, Entry, Error};

    use super::*;

    struct TestStore {
        mock: Arc<mock::Store>,
        attributes: Arc<Mutex<HashMap<String, String>>>,
        schema_error: bool,
    }

    impl CredentialStoreApi for TestStore {
        fn vendor(&self) -> String {
            self.mock.vendor()
        }

        fn id(&self) -> String {
            self.mock.id()
        }

        fn build(
            &self,
            service: &str,
            user: &str,
            modifiers: Option<&HashMap<&str, &str>>,
        ) -> keyring_core::Result<Entry> {
            Ok(Entry::new_with_credential(Arc::new(TestCredential {
                inner: self.mock.build(service, user, modifiers)?,
                attributes: self.attributes.clone(),
                schema_error: self.schema_error,
            })))
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    struct TestCredential {
        inner: Entry,
        attributes: Arc<Mutex<HashMap<String, String>>>,
        schema_error: bool,
    }

    impl CredentialApi for TestCredential {
        fn set_secret(&self, secret: &[u8]) -> keyring_core::Result<()> {
            self.inner.set_secret(secret)
        }

        fn get_secret(&self) -> keyring_core::Result<Vec<u8>> {
            self.inner.get_secret()
        }

        fn delete_credential(&self) -> keyring_core::Result<()> {
            self.inner.delete_credential()
        }

        fn get_credential(&self) -> keyring_core::Result<Option<Arc<Credential>>> {
            self.inner.get_credential()?;
            Ok(None)
        }

        fn get_specifiers(&self) -> Option<(String, String)> {
            self.inner.get_specifiers()
        }

        fn update_attributes(&self, attributes: &HashMap<&str, &str>) -> keyring_core::Result<()> {
            if self.schema_error {
                return Err(Error::NotSupportedByStore(
                    "test schema failure".to_string(),
                ));
            }

            self.attributes.lock().expect("attribute lock").extend(
                attributes
                    .iter()
                    .map(|(&key, &value)| (key.to_string(), value.to_string())),
            );
            Ok(())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn fixture(schema_error: bool) -> (PlatformQrzSecretStore, Arc<TestStore>) {
        let store = Arc::new(TestStore {
            mock: mock::Store::new().expect("mock store"),
            attributes: Arc::new(Mutex::new(HashMap::new())),
            schema_error,
        });
        let credential_store: Arc<CredentialStore> = store.clone();
        (
            PlatformQrzSecretStore {
                store: OnceLock::from(Ok(credential_store)),
            },
            store,
        )
    }

    fn specifiers(secret: QrzSecret) -> (String, String) {
        #[cfg(target_os = "windows")]
        return (
            format!("qrz/{}", secret.account()),
            "com.treitforge.qsoripper".to_string(),
        );

        #[cfg(target_os = "macos")]
        return (
            "com.treitforge.qsoripper.qrz".to_string(),
            secret.account().to_string(),
        );

        #[cfg(all(unix, not(target_os = "macos")))]
        return ("qrz".to_string(), secret.account().to_string());
    }

    #[test]
    fn construction_does_not_open_the_platform_store() {
        assert!(PlatformQrzSecretStore::new().store.get().is_none());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn native_store_initializes_without_accessing_a_password() {
        let store = PlatformQrzSecretStore::new();
        let entry = store.entry(QrzSecret::XmlPassword).expect("native entry");
        assert_eq!(
            entry.get_specifiers(),
            Some(specifiers(QrzSecret::XmlPassword))
        );
        assert!(store
            .store
            .get()
            .expect("initialized store")
            .as_ref()
            .expect("native store")
            .as_any()
            .is::<windows_native_keyring_store::Store>());
    }

    #[test]
    fn missing_credentials_return_none() {
        let (store, _) = fixture(false);
        assert_eq!(store.get(QrzSecret::XmlPassword), Ok(None));
        assert_eq!(store.get(QrzSecret::LogbookApiKey), Ok(None));
    }

    #[test]
    fn credential_identifiers_match_existing_records() {
        let (store, _) = fixture(false);
        for secret in [QrzSecret::XmlPassword, QrzSecret::LogbookApiKey] {
            assert_eq!(
                store.entry(secret).expect("entry").get_specifiers(),
                Some(specifiers(secret))
            );
        }
    }

    #[test]
    fn reads_credentials_written_with_existing_identifiers() {
        let (store, backend) = fixture(false);
        let (service, user) = specifiers(QrzSecret::XmlPassword);
        backend
            .mock
            .build(&service, &user, None)
            .expect("existing entry")
            .set_password("existing-test-password")
            .expect("existing password");
        assert_eq!(
            store.get(QrzSecret::XmlPassword),
            Ok(Some("existing-test-password".to_string()))
        );
    }

    #[test]
    fn writes_and_updates_separate_credentials() {
        let (store, _) = fixture(false);
        store
            .set(QrzSecret::XmlPassword, "test-password")
            .expect("password");
        store
            .set(QrzSecret::LogbookApiKey, "test-api-key")
            .expect("API key");
        store
            .set(QrzSecret::XmlPassword, "updated-test-password")
            .expect("update");
        assert_eq!(
            store.get(QrzSecret::XmlPassword),
            Ok(Some("updated-test-password".to_string()))
        );
        assert_eq!(
            store.get(QrzSecret::LogbookApiKey),
            Ok(Some("test-api-key".to_string()))
        );
    }

    #[test]
    fn credential_stores_do_not_share_global_state() {
        let (first, _) = fixture(false);
        let (second, _) = fixture(false);
        first
            .set(QrzSecret::XmlPassword, "test-password")
            .expect("password");
        assert_eq!(second.get(QrzSecret::XmlPassword), Ok(None));
    }

    #[test]
    fn initialization_errors_fail_reads_and_writes() {
        let message = "Platform credential store is unavailable: test initialization failure";
        let store = PlatformQrzSecretStore {
            store: OnceLock::from(Err(message.to_string())),
        };
        assert_eq!(store.get(QrzSecret::XmlPassword), Err(message.to_string()));
        assert_eq!(
            store.set(QrzSecret::XmlPassword, "test-password"),
            Err(message.to_string())
        );
    }

    #[test]
    fn credential_read_and_write_errors_are_reported() {
        let (store, backend) = fixture(false);
        let (service, user) = specifiers(QrzSecret::XmlPassword);
        let entry = backend
            .mock
            .build(&service, &user, None)
            .expect("mock entry");
        let credential = entry
            .as_any()
            .downcast_ref::<mock::Cred>()
            .expect("mock credential");
        credential.set_error(Error::NotSupportedByStore("test read failure".to_string()));
        assert!(store
            .get(QrzSecret::XmlPassword)
            .expect_err("read error")
            .starts_with("Failed to read the platform credential store:"));
        credential.set_error(Error::NotSupportedByStore("test write failure".to_string()));
        assert!(store
            .set(QrzSecret::XmlPassword, "test-password")
            .expect_err("write error")
            .starts_with("Failed to write the platform credential store:"));
    }

    #[test]
    fn writes_preserve_the_shared_secret_service_schema() {
        let (store, backend) = fixture(false);
        let entry = store.entry(QrzSecret::XmlPassword).expect("entry");
        entry.set_password("test-password").expect("password");
        update_secret_service_schema(&entry).expect("schema");
        assert_eq!(
            backend
                .attributes
                .lock()
                .expect("attributes")
                .get("xdg:schema"),
            Some(&"com.treitforge.qsoripper".to_string())
        );
    }

    #[test]
    fn schema_update_errors_are_reported() {
        let (store, _) = fixture(true);
        let entry = store.entry(QrzSecret::XmlPassword).expect("entry");
        assert!(update_secret_service_schema(&entry)
            .expect_err("schema error")
            .starts_with("Failed to set the platform credential store schema:"));
    }
}
