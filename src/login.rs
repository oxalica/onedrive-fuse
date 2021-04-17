use crate::config::de_duration_sec;
use anyhow::{ensure, Context as _, Result};
use onedrive_api::{Auth, DriveLocation, OneDrive, Permission};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};
use tokio::{
    self,
    sync::{RwLock, RwLockReadGuard},
};

#[derive(Debug, Deserialize)]
pub struct ReloginConfig {
    enable: bool,
    #[serde(deserialize_with = "de_duration_sec")]
    check_period: Duration,
    #[serde(deserialize_with = "de_duration_sec")]
    time_before_expire: Duration,
    #[serde(deserialize_with = "de_duration_sec")]
    min_live_time: Duration,
}

#[derive(Clone)]
pub struct ManagedOnedrive {
    onedrive: Arc<RwLock<OneDrive>>,
}

impl ManagedOnedrive {
    pub async fn login(
        credential_file: PathBuf,
        config: ReloginConfig,
        mount_readonly: bool,
    ) -> Result<Self> {
        log::info!("Logining...");
        let mut cred = Credential::load(&credential_file)
            .context("Invalid credential file. Try to re-login.")?;
        ensure!(
            !cred.readonly || mount_readonly,
            "Cannot mount as read-write using read-only token. Please re-login to grant read-write permission.",
        );
        let auth = Auth::new(
            cred.client_id.clone(),
            Permission::new_read()
                .write(!cred.readonly)
                .offline_access(true),
            cred.redirect_uri.clone(),
        );
        let resp = auth
            .login_with_refresh_token(&cred.refresh_token, None)
            .await?;
        log::info!(
            "Logined. Token will be expired in {} s.",
            resp.expires_in_secs
        );

        cred.refresh_token = resp.refresh_token.unwrap();
        cred.save(&credential_file)?;
        log::info!("New credential saved");

        let onedrive = Arc::new(RwLock::new(OneDrive::new(
            resp.access_token,
            DriveLocation::me(),
        )));

        if config.enable {
            tokio::spawn(Self::relogin_thread(
                Arc::downgrade(&onedrive),
                auth,
                cred,
                credential_file,
                Duration::from_secs(resp.expires_in_secs),
                config,
            ));
        }

        Ok(Self { onedrive })
    }

    async fn relogin_thread(
        weak: Weak<RwLock<OneDrive>>,
        auth: Auth,
        mut cred: Credential,
        credential_file: PathBuf,
        initial_expire_time: Duration,
        config: ReloginConfig,
    ) {
        let login_time = SystemTime::now();
        let mut relogin_inst = std::cmp::max(
            login_time + initial_expire_time - config.time_before_expire,
            login_time + config.min_live_time,
        );
        log::info!(
            "Next relogin will happen after {}",
            humantime::Timestamp::from(relogin_inst),
        );

        loop {
            tokio::time::sleep(config.check_period).await;
            if SystemTime::now() < relogin_inst {
                continue;
            }

            let onedrive = match weak.upgrade() {
                Some(onedrive) => onedrive,
                None => return,
            };

            log::info!("Relogining...");
            let resp = match auth
                .login_with_refresh_token(&cred.refresh_token, None)
                .await
            {
                Err(err) => {
                    log::error!("Relogin failed (will retry in next period): {:?}", err);
                    continue;
                }
                Ok(resp) => resp,
            };
            let login_time = SystemTime::now();
            relogin_inst = std::cmp::max(
                login_time + Duration::from_secs(resp.expires_in_secs) - config.time_before_expire,
                login_time + config.min_live_time,
            );

            *onedrive.write().await = OneDrive::new(resp.access_token, DriveLocation::me());

            log::info!(
                "Relogined. Next relogin will happen after {}",
                humantime::Timestamp::from(relogin_inst),
            );

            cred.refresh_token = resp.refresh_token.unwrap();
            match cred.save(&credential_file) {
                Ok(()) => log::info!("New credential saved"),
                Err(err) => log::error!(
                    "Cannot save credential file. Your refresh token may expire! {}",
                    err,
                ),
            }
        }
    }

    pub async fn get(&self) -> RwLockReadGuard<'_, OneDrive> {
        self.onedrive.read().await
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Credential {
    pub readonly: bool,
    pub client_id: String,
    pub redirect_uri: String,
    pub refresh_token: String,
}

impl Credential {
    pub fn load(path: &Path) -> Result<Self> {
        let f = fs::File::open(path)?;
        Ok(serde_json::from_reader(f)?)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        let parent = path.parent().context("Invalid credential path")?;
        fs::create_dir_all(parent)?;

        let tmp_path = if path.extension().map_or(false, |ext| ext == "tmp") {
            path.with_extension("_tmp")
        } else {
            path.with_extension("tmp")
        };

        {
            let f = fs::File::create(&tmp_path)?;
            f.set_permissions(fs::Permissions::from_mode(0o600)) // rw-------
                .context("Cannot set permission of credential file")?;
            serde_json::to_writer(f, self)?;
        }

        fs::rename(&tmp_path, path)?;
        Ok(())
    }
}
