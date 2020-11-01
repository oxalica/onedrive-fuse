use crate::{
    config::de_duration_sec,
    login::ManagedOnedrive,
    vfs::{self, inode::InodeAttr},
};
use onedrive_api::{
    option::CollectionOption,
    resource::{DriveItem, DriveItemField},
    ItemLocation, OneDrive,
};
use serde::Deserialize;
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::{Duration, Instant},
};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    enable: bool,
    #[serde(deserialize_with = "de_duration_sec")]
    period: Duration,
    fetch_page_size: NonZeroUsize,
    max_changes: usize,
}

pub type EventHandler = Box<dyn FnMut(Event) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Tracker {
    last_sync_time: Option<Arc<SyncMutex<Instant>>>,
    config: Config,
}

#[derive(Debug)]
pub enum Event {
    /// Clear current states.
    Clear,
    /// Update on old states.
    Update(Vec<DriveItem>),
}

impl Tracker {
    pub async fn new(
        handler: EventHandler,
        onedrive: ManagedOnedrive,
        config: Config,
    ) -> vfs::Result<Self> {
        if !config.enable {
            return Ok(Self {
                last_sync_time: None,
                config,
            });
        }

        let last_sync_time = Arc::new(SyncMutex::new(Instant::now()));
        let delta_url = get_delta_url_with_option(&*onedrive.get().await, &config).await?;
        tokio::spawn(tracking_thread(
            handler,
            delta_url,
            onedrive,
            Arc::downgrade(&last_sync_time),
            config.clone(),
        ));

        Ok(Self {
            last_sync_time: Some(last_sync_time),
            config,
        })
    }

    pub fn time_to_next_sync(&self) -> Option<Duration> {
        let passed = self.last_sync_time.as_ref()?.lock().unwrap().elapsed();
        // Zero if time exceeded.
        Some(self.config.period.checked_sub(passed).unwrap_or_default())
    }
}

async fn tracking_thread(
    mut handler: EventHandler,
    mut delta_url: String,
    onedrive: ManagedOnedrive,
    last_sync_time: Weak<SyncMutex<Instant>>,
    config: Config,
) {
    log::info!("Tracking thread started");
    let mut interval = tokio::time::interval(config.period);
    interval.tick().await;
    loop {
        interval.tick().await;
        let start_time = Instant::now();

        let onedrive = onedrive.get().await;
        let need_clear = match sync_changes(&mut handler, &mut delta_url, &onedrive, &config).await
        {
            Ok(need_clear) => need_clear,
            // Client error 410 Gone (resyncRequired) indicates our `delta_url` is expired.
            Err(err) if err.status_code().map_or(false, |st| st.is_client_error()) => {
                log::info!("Re-sync required. Delta URL is gone: {}", err);
                true
            }
            // Maybe network error. Try again.
            Err(err) => {
                log::error!("Failed to fetch changes: {}", err);
                continue;
            }
        };

        if need_clear {
            log::warn!("Cache cleared");
            // Retrive new delta url before the reset to ensure changes between them will be caught.
            let ret = get_delta_url_with_option(&onedrive, &config).await;
            // No matter what the result is, cache must be cleared.
            handler(Event::Clear).await;
            match ret {
                Ok(url) => delta_url = url,
                // Error. Try again.
                Err(err) => {
                    log::error!("Failed to retrive new delta url: {}", err);
                    continue;
                }
            }
        }

        match last_sync_time.upgrade() {
            Some(arc) => *arc.lock().unwrap() = start_time,
            None => return,
        };
    }
}

async fn get_delta_url_with_option(
    onedrive: &OneDrive,
    config: &Config,
) -> onedrive_api::Result<String> {
    // FIXME: Pass options from vfs.
    Ok(onedrive
        .get_latest_delta_url_with_option(
            ItemLocation::root(),
            CollectionOption::new()
                .page_size(config.fetch_page_size.into())
                .select(&[
                    DriveItemField::id,
                    DriveItemField::name,
                    DriveItemField::deleted,
                    DriveItemField::root,
                    DriveItemField::parent_reference,
                ])
                .select(InodeAttr::ATTR_SELECT_FIELDS),
        )
        .await?)
}

/// Fetch and sync changes.
///
/// Return whether need to clear cache to re-sync.
async fn sync_changes(
    handler: &mut EventHandler,
    delta_url: &mut String,
    onedrive: &OneDrive,
    config: &Config,
) -> onedrive_api::Result<bool> {
    log::trace!("Fetching and synchronizing changes...");

    let mut fetcher = onedrive.track_changes_from_delta_url(&delta_url).await?;

    let mut total_changes = 0;
    while let Some(changes) = fetcher.fetch_next_page(&onedrive).await? {
        total_changes += changes.len();
        if config.max_changes < total_changes {
            log::warn!(
                "Too many changes: got more than {} changes (limit: {})",
                total_changes,
                config.max_changes,
            );
            return Ok(true);
        }
        if !changes.is_empty() {
            handler(Event::Update(changes)).await;
        }
    }

    if total_changes == 0 {
        log::trace!("Nothing changed on remote side");
    } else {
        log::info!("Synchronized {} changes", total_changes);
    }

    *delta_url = fetcher.delta_url().unwrap().to_owned();
    Ok(false)
}
