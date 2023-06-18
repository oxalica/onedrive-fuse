use std::collections::HashMap;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{ensure, Context};
use fuser::{FileAttr, FileType};
use futures::future::Either;
use futures::{AsyncRead, AsyncReadExt, Future, StreamExt};
use nix::errno::Errno;
use rusqlite::{
    named_params, params, Connection, DropBehavior, OptionalExtension, TransactionBehavior,
};

mod backend;
mod fuse_fs;

pub use fuse_fs::FuseFs;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use crate::config::PermissionConfig;

pub use self::backend::{Backend, FullSyncRequired, OnedriveBackend};
use self::backend::{LocalItemChange, RemoteItemChange};

// TODO: Configurable.
const SYNC_PERIOD: Duration = Duration::from_secs(60);

const BLOCK_SIZE: u32 = 512;

const _: [(); 1] = [(); (fuser::FUSE_ROOT_ID == 1) as usize];

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // User errors.
    #[error("object not found")]
    NotFound,
    #[error("non-sequential read on streams")]
    NonsequentialRead,
    #[error("object already exists")]
    Exists,

    // API errors.
    #[error("network error during download")]
    Network(#[source] anyhow::Error),
    #[error("read stream poisoned")]
    Poisoned,
}

impl From<Error> for i32 {
    fn from(err: Error) -> i32 {
        let err = match err {
            Error::NotFound => Errno::ENOENT,
            Error::NonsequentialRead => Errno::ESPIPE,
            Error::Exists => Errno::EEXIST,

            Error::Network(err) => {
                log::error!("{err:#}");
                Errno::EIO
            }
            Error::Poisoned => Errno::EIO,
        };
        err as _
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        if let rusqlite::Error::QueryReturnedNoRows = err {
            return Self::NotFound;
        }
        panic!("sqlite error: {err}");
    }
}

pub struct Vfs<B> {
    conn: Connection,
    permission: PermissionConfig,
    backend: B,
    file_streams: HashMap<u64, FileStreams>,

    sync_thread: task::JoinHandle<()>,
    sync_trigger: mpsc::Sender<SyncCallback>,
    last_sync_timestamp: Arc<AtomicU64>,
}

type SyncCallback = oneshot::Sender<anyhow::Result<()>>;

struct FileStreams {
    ref_count: usize,
    id: String,
    size: u64,
    // stream_position -> stream
    readers: Arc<Mutex<HashMap<u64, Reader>>>,
}

type Reader = Pin<Box<dyn AsyncRead + Send + 'static>>;

impl<B> Drop for Vfs<B> {
    fn drop(&mut self) {
        self.sync_thread.abort();
    }
}

struct RemoteChangeCommitter<'conn> {
    insert_root_stmt: rusqlite::Statement<'conn>,
    insert_child_stmt: rusqlite::Statement<'conn>,
    delete_stmt: rusqlite::Statement<'conn>,
}

impl<'conn> RemoteChangeCommitter<'conn> {
    fn new(conn: &'conn rusqlite::Connection) -> rusqlite::Result<Self> {
        let insert_root_stmt = conn.prepare(
            r"
            INSERT OR REPLACE INTO item (ino, id, is_directory, parent_ino, name, size, created_time, modified_time)
            VALUES (1, :id, TRUE, NULL, NULL, 0, :created_time, :modified_time)
            ",
        )?;
        let insert_child_stmt = conn.prepare(
            r"
            INSERT INTO item
            (id, is_directory, parent_ino, name, size, created_time, modified_time)
            SELECT :id, :is_directory, ino, :name, :size, :created_time, :modified_time
            FROM item
            WHERE id = :parent_id AND is_directory
            ON CONFLICT (id) DO UPDATE -- Remote updates.
            SET
                name = :name,
                size = :size,
                is_directory = :is_directory,
                parent_ino = excluded.parent_ino,
                created_time = :created_time,
                modified_time = :modified_time
            ON CONFLICT (parent_ino, name) DO UPDATE -- Pushed new items.
            SET
                id = :id,
                size = :size,
                is_directory = :is_directory,
                created_time = :created_time,
                modified_time = :modified_time
            ",
        )?;
        let delete_stmt = conn.prepare(
            r"
            DELETE FROM item
            WHERE id = :id
            ",
        )?;
        Ok(Self {
            insert_root_stmt,
            insert_child_stmt,
            delete_stmt,
        })
    }

    fn execute(&mut self, change: &RemoteItemChange) -> rusqlite::Result<()> {
        let to_timestamp = |time: SystemTime| {
            time.duration_since(SystemTime::UNIX_EPOCH)
                .expect("post UNIX epoch")
                .as_secs()
        };

        match change {
            RemoteItemChange::RootUpdate {
                id,
                created_time,
                modified_time,
            } => {
                self.insert_root_stmt.execute(named_params! {
                    ":id": id,
                    ":created_time": to_timestamp(*created_time),
                    ":modified_time": to_timestamp(*modified_time),
                })?;
            }
            RemoteItemChange::Update {
                id,
                parent_id,
                name,
                is_directory,
                size,
                created_time,
                modified_time,
            } => {
                self.insert_child_stmt.execute(named_params! {
                    ":id": id,
                    ":parent_id": parent_id,
                    ":name": name,
                    ":is_directory": is_directory,
                    ":size": size,
                    ":created_time": to_timestamp(*created_time),
                    ":modified_time": to_timestamp(*modified_time),
                })?;
            }
            RemoteItemChange::Delete { id } => {
                self.delete_stmt.execute(named_params! {
                    ":id": id,
                })?;
            }
        }
        Ok(())
    }
}

impl<B: Backend> Vfs<B> {
    const INIT_SQL: &'static str = include_str!("./init.sql");

    pub async fn new(
        backend: B,
        mut conn: Connection,
        permission: PermissionConfig,
    ) -> anyhow::Result<Self> {
        conn.execute_batch(Self::INIT_SQL)?;

        // Try to clone the connection and report possible errors early.
        // XXX: This is a bit fishy. Could we do better?
        let path = conn.path().context("missing database path")?;
        let sync_conn = Connection::open(path).context("failed to reconnect to the database")?;

        // Apply initial changes first.
        let init_timestamp = Self::pull_remote_changes(&mut conn, &backend).await?;

        let (sync_trigger, sync_trigger_rx) = mpsc::channel(1);
        let last_sync_timestamp = Arc::new(AtomicU64::new(init_timestamp));
        // Workaround: `Transaction` is `!Send` and kept across `await` point, causing the future
        // to be `!Send`.
        let sync_thread = task::spawn_local(Self::sync_thread(
            sync_conn,
            backend.clone(),
            sync_trigger_rx,
            Arc::clone(&last_sync_timestamp),
        ));

        Ok(Self {
            conn,
            permission,
            backend,
            file_streams: HashMap::new(),
            sync_thread,
            sync_trigger,
            last_sync_timestamp,
        })
    }

    async fn sync_thread(
        mut conn: Connection,
        backend: B,
        mut sync_trigger: mpsc::Receiver<SyncCallback>,
        last_sync_timestamp: Arc<AtomicU64>,
    ) {
        loop {
            let callback = match tokio::time::timeout(SYNC_PERIOD, sync_trigger.recv()).await {
                Ok(Some(callback)) => Some(callback),
                Err(_elapsed) => None,
                Ok(None) => return,
            };

            // XXX: Balance between push and pull to prevent starvation?
            loop {
                match Self::push_local_changes(&mut conn, &backend).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(err) => {
                        // TODO: Make the filesystem readonly?
                        log::error!("Failed to push local changes: {err:#}");
                        break;
                    }
                }
            }

            let ret = Self::pull_remote_changes(&mut conn, &backend).await;
            match &ret {
                Ok(timestamp) => {
                    last_sync_timestamp.store(*timestamp, Ordering::Relaxed);
                }
                Err(err) => {
                    // TODO: Make the filesystem readonly?
                    log::error!("Failed to pull remote changes: {err:#}");
                }
            }
            if let Some(callback) = callback {
                let _: Result<_, _> = callback.send(ret.map(drop));
            };
        }
    }

    async fn push_local_changes(conn: &mut Connection, backend: &B) -> anyhow::Result<usize> {
        let mut txn = conn.transaction()?;

        // If some requests failed, still commit the success ones.
        txn.set_drop_behavior(DropBehavior::Commit);

        let max_len = backend.max_push_len();
        let changes = txn
            .prepare(
                r"
                WITH RECURSIVE
                    dirty_item(ino, parent_id, child_path, created_time, modified_time) AS (
                        SELECT
                            item.ino,
                            parent.id,
                            '/' || item.name,
                            item.created_time,
                            item.modified_time
                        FROM item
                        JOIN item AS parent ON parent.ino = item.parent_ino AND parent.id IS NOT NULL
                        WHERE item.id IS NULL

                        UNION ALL
                        SELECT
                            item.ino,
                            parent_id,
                            child_path || '/' || item.name,
                            item.created_time,
                            item.modified_time
                        FROM dirty_item
                        JOIN item ON item.parent_ino = dirty_item.ino
                    )
                SELECT * FROM dirty_item
                LIMIT ?
                ",
            )?
            .query_and_then(params![max_len], |row| {
                anyhow::Ok(LocalItemChange::CreateDirectory {
                    parent_id: row.get("parent_id")?,
                    child_path: row.get("child_path")?,
                    created_time: SystemTime::UNIX_EPOCH
                        + Duration::from_secs(row.get("created_time")?),
                    modified_time: SystemTime::UNIX_EPOCH
                        + Duration::from_secs(row.get("modified_time")?),
                })
            })?
            .collect::<anyhow::Result<Vec<_>>>()?;

        // Fast path.
        if changes.is_empty() {
            return Ok(0);
        }

        let responses = backend.push_changes(changes).await?;
        let responses_len = responses.len();
        ensure!(responses_len != 0, "no response");
        {
            let mut committer = RemoteChangeCommitter::new(&txn)?;
            for ret in responses {
                // TODO: Make this fail-safe.
                committer.execute(&ret?)?;
            }
        }
        log::info!("Pushed {responses_len} local changes");

        txn.commit()?;
        Ok(responses_len)
    }

    async fn pull_remote_changes(conn: &mut Connection, backend: &B) -> anyhow::Result<u64> {
        let txn = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let prev_delta_url = txn
            .query_row(r"SELECT * FROM sync LIMIT 1", params![], |row| {
                row.get::<_, String>("delta_url")
            })
            .optional()?;

        let mut is_full_sync = prev_delta_url.is_none();

        let mut stream = match backend.pull_changes(prev_delta_url).await {
            Ok(stream) => stream,
            Err(err) if err.is::<FullSyncRequired>() => {
                log::warn!("{err}");
                is_full_sync = true;
                backend.pull_changes(None).await?
            }
            Err(err) => return Err(err),
        };

        let fetch_timestamp = current_timestamp();

        if is_full_sync {
            txn.execute(r"DELETE FROM item", params![])?;
        }

        let mut change_cnt = 0u64;
        let new_delta_url = {
            let mut committer = RemoteChangeCommitter::new(&txn)?;
            loop {
                let change = match stream.next().await.context("missing delta_url")?? {
                    Either::Left(item) => item,
                    Either::Right(delta_url) => break delta_url,
                };

                change_cnt += 1;
                if change_cnt % 1024 == 0 {
                    log::info!("Got {change_cnt} remote changes...");
                }

                committer.execute(&change)?;
            }
        };
        if change_cnt != 0 {
            log::info!("Applied {change_cnt} remote changes in total");
        } else {
            log::info!("No changes");
        }

        txn.execute(r"DELETE FROM sync", params![])?;
        txn.execute(
            r"INSERT INTO sync (time, delta_url) VALUES (?, ?)",
            params![fetch_timestamp, new_delta_url],
        )?;

        txn.commit()?;

        Ok(fetch_timestamp)
    }

    fn ttl(&self) -> Duration {
        let secs = self
            .last_sync_timestamp
            .load(Ordering::Relaxed)
            .saturating_add(SYNC_PERIOD.as_secs())
            // Additional 1sec more TTL for the subsecond part.
            .saturating_add(1)
            .saturating_sub(current_timestamp());
        Duration::from_secs(secs)
    }

    fn parse_attr(&self, row: &rusqlite::Row<'_>) -> FileAttr {
        let ino = row.get_unwrap("ino");
        let is_dir = row.get_unwrap("is_directory");
        let (kind, perm) = if is_dir {
            (FileType::Directory, self.permission.dir_permission())
        } else {
            (FileType::RegularFile, self.permission.file_permission())
        };
        let size = row.get_unwrap::<_, Option<u64>>("size").unwrap_or(0);
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(row.get_unwrap("modified_time"));
        let crtime = SystemTime::UNIX_EPOCH + Duration::from_secs(row.get_unwrap("created_time"));
        FileAttr {
            ino,
            size,
            blocks: (size + (u64::from(BLOCK_SIZE) - 1)) / u64::from(BLOCK_SIZE),
            mtime,
            crtime,
            atime: mtime,
            ctime: mtime,
            kind,
            perm: perm as _,
            nlink: 1,
            uid: self.permission.uid,
            gid: self.permission.gid,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    pub fn lookup(&self, parent_ino: u64, child_name: &str) -> Result<(FileAttr, Duration)> {
        Ok(self.conn.query_row(
            r"SELECT * FROM item WHERE parent_ino = ? AND name = ?",
            params![parent_ino, child_name],
            |row| {
                let attr = self.parse_attr(row);
                Ok((attr, self.ttl()))
            },
        )?)
    }

    pub fn getattr(&mut self, ino: u64) -> Result<(FileAttr, Duration)> {
        Ok(self
            .conn
            .query_row(r"SELECT * FROM item WHERE ino = ?", params![ino], |row| {
                let attr = self.parse_attr(row);
                Ok((attr, self.ttl()))
            })?)
    }

    pub fn read_dir(
        &self,
        ino: u64,
        offset: u64,
        mut f: impl FnMut(&str, FileAttr, Duration) -> ControlFlow<()>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT * FROM item
            WHERE parent_ino = ? AND ino > ?
            ORDER BY ino ASC
            ",
        )?;
        let ttl = self.ttl();
        let mut rows = stmt.query(params![ino, offset])?;
        while let Some(row) = rows.next()? {
            let name = row
                .get_ref_unwrap("name")
                .as_str()
                .map_err(rusqlite::Error::from)?;
            if f(name, self.parse_attr(row), ttl).is_break() {
                break;
            }
        }
        Ok(())
    }

    pub fn open_file(&mut self, ino: u64) -> Result<()> {
        use std::collections::hash_map::Entry;

        let (size, id): (u64, String) = self.conn.query_row(
            r"
            SELECT size, id
            FROM item
            WHERE ino = ? AND NOT is_directory
            ",
            params![ino],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        match self.file_streams.entry(ino) {
            Entry::Occupied(mut ent) => {
                ent.get_mut().ref_count += 1;
            }
            Entry::Vacant(ent) => {
                ent.insert(FileStreams {
                    ref_count: 1,
                    id,
                    size,
                    readers: Arc::new(Mutex::new(HashMap::new())),
                });
            }
        }

        Ok(())
    }

    pub fn close_file(&mut self, ino: u64) {
        if let Some(stream) = self.file_streams.get_mut(&ino) {
            stream.ref_count -= 1;
            if stream.ref_count == 0 {
                self.file_streams.remove(&ino);
                log::debug!("drop streams of ino {ino:#x}");
            }
        }
    }

    pub fn read_file(
        &mut self,
        ino: u64,
        offset: u64,
        mut len: usize,
    ) -> Result<impl Future<Output = Result<Vec<u8>>> + 'static> {
        assert_ne!(len, 0, "kernel should not read zero");

        let streams = self.file_streams.get_mut(&ino).ok_or(Error::NotFound)?;
        if streams.size <= offset {
            return Ok(Either::Left(futures::future::ready(Ok(Vec::new()))));
        }
        // Clamp the length.
        len = usize::try_from(streams.size - offset)
            .unwrap_or(usize::MAX)
            .min(len);
        let reader = streams.readers.lock().unwrap().remove(&offset);
        let mut reader =
            reader.unwrap_or_else(|| self.backend.download(streams.id.clone(), offset));

        let readers = Arc::downgrade(&streams.readers);
        Ok(Either::Right(async move {
            let mut buf = vec![0u8; len];
            reader
                .as_mut()
                .read_exact(&mut buf)
                .await
                .map_err(|err| Error::Network(err.into()))?;
            if let Some(readers) = readers.upgrade() {
                let new_pos = offset + len as u64;
                readers.lock().unwrap().insert(new_pos, reader);
            }
            Ok(buf)
        }))
    }

    pub fn sync(&mut self) -> impl Future<Output = Result<()>> + 'static {
        let (ret_tx, ret_rx) = oneshot::channel();
        let sync_trigger = self.sync_trigger.clone();
        async move {
            sync_trigger
                .send(ret_tx)
                .await
                .expect("sync thread aborted");
            ret_rx
                .await
                .expect("sync thread aborted")
                .context("sync failed")
                .map_err(Error::Network)
        }
    }

    pub fn create_directory(
        &mut self,
        parent_ino: u64,
        child_name: &str,
    ) -> Result<(Duration, FileAttr)> {
        let timestamp = current_timestamp();

        let changed = self.conn.execute(
            r"
            INSERT OR IGNORE
            INTO item (id, is_directory, parent_ino, name, size, created_time, modified_time)
            VALUES (NULL, TRUE, :parent_ino, :name, 0, :timestamp, :timestamp)
            ",
            named_params! {
                ":parent_ino": parent_ino,
                ":name": child_name,
                ":timestamp": timestamp,
            },
        )?;
        if changed == 0 {
            return Err(Error::Exists);
        }
        let ino = self.conn.last_insert_rowid() as u64;

        // XXX: Should we cache items not uploaded yet?
        let ttl = Duration::ZERO;
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);
        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::Directory,
            perm: self.permission.dir_permission() as _,
            nlink: 1,
            uid: self.permission.uid,
            gid: self.permission.gid,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        };
        Ok((ttl, attr))
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("post epoch")
        .as_secs()
}
