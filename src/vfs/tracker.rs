use crate::{config::de_duration_sec, login::ManagedOnedrive, vfs::UpdateEvent};
use onedrive_api::{
    option::CollectionOption,
    resource::{DriveItem, DriveItemField},
    OneDrive,
};
use serde::Deserialize;
use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    enable: bool,
    #[serde(deserialize_with = "de_duration_sec")]
    period: Duration,
    fetch_page_size: NonZeroUsize,
}

pub struct Tracker {
    last_sync_time: Option<Arc<SyncMutex<Instant>>>,
    config: Config,
}

impl Tracker {
    pub async fn new(
        event_tx: mpsc::Sender<UpdateEvent>,
        select_fields: Vec<DriveItemField>,
        onedrive: ManagedOnedrive,
        config: Config,
    ) -> anyhow::Result<Self> {
        let (weak, last_sync_time) = match config.enable {
            false => (Weak::new(), None),
            true => {
                let arc = Arc::new(SyncMutex::new(Instant::now()));
                (Arc::downgrade(&arc), Some(arc))
            }
        };

        tokio::spawn(tracking_thread(
            None,
            event_tx,
            select_fields,
            onedrive,
            weak,
            config.clone(),
        ));

        Ok(Self {
            last_sync_time,
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
    mut delta_url: Option<String>,
    event_tx: mpsc::Sender<UpdateEvent>,
    select_fields: Vec<DriveItemField>,
    onedrive: ManagedOnedrive,
    last_sync_time: Weak<SyncMutex<Instant>>,
    config: Config,
) {
    log::debug!("Tracking thread started");
    let mut interval = tokio::time::interval(config.period);

    loop {
        // The first tick doesn't wait.
        interval.tick().await;
        let start_time = Instant::now();

        let onedrive = onedrive.get().await;

        match fetch_changes(&mut delta_url, &select_fields, &onedrive, &config).await {
            Ok(Some(changes)) => {
                if event_tx
                    .send(UpdateEvent::BatchUpdate(changes))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            // Wait for the next scan.
            Ok(None) => continue,
            Err(err) => {
                log::error!("Failed to fetch changes: {}", err);
                continue;
            }
        }

        match last_sync_time.upgrade() {
            Some(arc) => *arc.lock().unwrap() = start_time,
            None => return,
        }
    }
}

/// Fetch initial or delta changes with optional progress.
///
/// Returns `Some(changes)` or `None` when delta url is gone.
async fn fetch_changes(
    delta_url: &mut Option<String>,
    select_fields: &[DriveItemField],
    onedrive: &OneDrive,
    config: &Config,
) -> onedrive_api::Result<Option<Vec<DriveItem>>> {
    let mut fetcher = match delta_url {
        // First fetch.
        None => {
            log::info!("Fetching metadata of the whole tree...");
            let opt = CollectionOption::new()
                .page_size(config.fetch_page_size.into())
                .select(&[DriveItemField::id])
                .select(select_fields);
            onedrive
                .track_root_changes_from_initial_with_option(opt)
                .await?
        }
        // Delta fetch.
        Some(url) => {
            log::debug!("Checking remote changes");
            match onedrive.track_root_changes_from_delta_url(url).await {
                Ok(fetcher) => fetcher,
                Err(err) if err.status_code().map_or(false, |st| st.is_client_error()) => {
                    log::info!("Re-sync required. Delta URL is gone: {}", err);
                    *delta_url = None;
                    return Ok(None);
                }
                Err(err) => return Err(err),
            }
        }
    };

    let mut page = 0usize;
    let mut total_changes = 0usize;
    let mut ret = Vec::new();
    let mut seen_ids = HashSet::new();
    while let Some(changes) = fetcher.fetch_next_page(&onedrive).await? {
        total_changes += changes.len();
        page += 1;

        // > The same item may appear more than once in a delta feed, for various reasons. You should use the last occurrence you see.
        // See: https://docs.microsoft.com/en-us/graph/api/driveitem-delta?view=graph-rest-1.0&tabs=http#remarks
        ret.extend(
            changes
                .into_iter()
                .filter(|item| seen_ids.insert(item.id.clone().unwrap())),
        );

        if page >= 2 {
            log::info!("Fetched {} changes...", total_changes);
        }
    }

    if total_changes != 0 {
        log::info!("Received {} changes in total", total_changes);

        if log::log_enabled!(log::Level::Trace) {
            use std::fmt::Write;
            let mut buf = String::new();
            for item in &ret {
                writeln!(buf, "    {:?}", item).unwrap();
            }
            log::trace!("Changes:\n{}", buf);
        }
    }

    *delta_url = Some(fetcher.delta_url().expect("Missing delta url").to_owned());

    Ok(Some(ret))
}
