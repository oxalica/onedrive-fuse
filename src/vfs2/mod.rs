use std::collections::HashMap;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use fuser::{FileAttr, FileType};
use futures::future::Either;
use futures::{AsyncRead, AsyncReadExt, Future, StreamExt};
use nix::errno::Errno;
use rusqlite::{named_params, params, Connection, OptionalExtension, TransactionBehavior};

mod backend;
mod fuse_fs;

pub use fuse_fs::FuseFs;

use crate::config::PermissionConfig;

use self::backend::ItemChange;
pub use self::backend::{Backend, FullSyncRequired, OnedriveBackend};

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

    // API errors.
    #[error("network error during download")]
    Network(#[source] std::io::Error),
    #[error("read stream poisoned")]
    Poisoned,
}

impl From<Error> for i32 {
    fn from(err: Error) -> i32 {
        let err = match err {
            Error::NotFound => Errno::ENOENT,
            Error::NonsequentialRead => Errno::ESPIPE,

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
}

struct FileStreams {
    ref_count: usize,
    id: String,
    size: u64,
    // stream_position -> stream
    readers: Arc<Mutex<HashMap<u64, Reader>>>,
}

type Reader = Pin<Box<dyn AsyncRead + Send + 'static>>;

impl<B: Backend> Vfs<B> {
    const INIT_SQL: &'static str = include_str!("./init.sql");

    pub async fn new(
        backend: B,
        conn: Connection,
        permission: PermissionConfig,
    ) -> anyhow::Result<Self> {
        conn.execute_batch(Self::INIT_SQL)?;
        let mut this = Self {
            conn,
            permission,
            backend,
            file_streams: HashMap::new(),
        };
        this.apply_remote_changes().await?;
        Ok(this)
    }

    async fn apply_remote_changes(&mut self) -> anyhow::Result<()> {
        let prev_delta_url = self
            .conn
            .query_row(r"SELECT * FROM sync LIMIT 1", params![], |row| {
                row.get::<_, String>("delta_url")
            })
            .optional()?;

        let mut is_full_sync = prev_delta_url.is_none();

        let mut stream = match self.backend.fetch_changes(prev_delta_url).await {
            Ok(stream) => stream,
            Err(err) if err.is::<FullSyncRequired>() => {
                log::warn!("{err}");
                is_full_sync = true;
                self.backend.fetch_changes(None).await?
            }
            Err(err) => return Err(err),
        };

        let fetch_timestamp = current_timestamp();

        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if is_full_sync {
            txn.execute(r"DELETE FROM item", params![])?;
        }

        let mut change_cnt = 0u64;
        let new_delta_url = {
            let mut insert_root_stmt = txn.prepare(
                r"
                INSERT OR REPLACE INTO item (ino, id, is_directory, parent_ino, name, size, created_time, modified_time)
                VALUES (1, :id, TRUE, NULL, NULL, 0, :created_time, :modified_time)
                ",
            )?;
            let mut insert_child_stmt = txn.prepare(
                r"
                INSERT INTO item
                (id, is_directory, parent_ino, name, size, created_time, modified_time)
                SELECT :id, :is_directory, ino, :name, :size, :created_time, :modified_time
                FROM item
                WHERE id = :parent_id AND is_directory
                ON CONFLICT DO UPDATE
                SET
                    name = :name,
                    size = :size,
                    parent_ino = excluded.parent_ino,
                    created_time = :created_time,
                    modified_time = :modified_time
                ",
            )?;
            let mut delete_stmt = txn.prepare(
                r"
                DELETE FROM item
                WHERE id = :id
                ",
            )?;

            loop {
                let change = match stream.next().await.context("missing delta_url")?? {
                    Either::Left(item) => item,
                    Either::Right(delta_url) => break delta_url,
                };

                change_cnt += 1;
                if change_cnt % 1024 == 0 {
                    log::info!("Got {change_cnt} remote changes...");
                }

                let to_timestamp = |time: SystemTime| {
                    time.duration_since(SystemTime::UNIX_EPOCH)
                        .expect("post UNIX epoch")
                        .as_secs()
                };

                match change {
                    ItemChange::RootUpdate {
                        id,
                        created_time,
                        modified_time,
                    } => {
                        insert_root_stmt.execute(named_params! {
                            ":id": id,
                            ":created_time": to_timestamp(created_time),
                            ":modified_time": to_timestamp(modified_time),
                        })?;
                    }
                    ItemChange::Update {
                        id,
                        parent_id,
                        name,
                        is_directory,
                        size,
                        created_time,
                        modified_time,
                    } => {
                        insert_child_stmt.execute(named_params! {
                            ":id": id,
                            ":parent_id": parent_id,
                            ":name": name,
                            ":is_directory": is_directory,
                            ":size": size,
                            ":created_time": to_timestamp(created_time),
                            ":modified_time": to_timestamp(modified_time),
                        })?;
                    }
                    ItemChange::Delete { id } => {
                        delete_stmt.execute(named_params! {
                            ":id": id,
                        })?;
                    }
                }
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

        Ok(())
    }

    // TODO: Auto sync.
    fn ttl(&self) -> Duration {
        Duration::MAX
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
                .map_err(Error::Network)?;
            if let Some(readers) = readers.upgrade() {
                let new_pos = offset + len as u64;
                readers.lock().unwrap().insert(new_pos, reader);
            }
            Ok(buf)
        }))
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("post epoch")
        .as_secs()
}
