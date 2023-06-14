use std::collections::HashMap;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{bail, ensure, Context};
use fuser::{FileAttr, FileType};
use futures::{AsyncRead, AsyncReadExt, Future};
use nix::errno::Errno;
use rusqlite::{named_params, params, Connection, OptionalExtension, TransactionBehavior};

mod backend;
mod fuse_fs;

pub use fuse_fs::FuseFs;

use crate::config::PermissionConfig;

pub use self::backend::{Backend, OnedriveBackend};

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
    file_streams: Arc<Mutex<HashMap<u64, FileStream>>>,
}

struct FileStream {
    ref_count: usize,
    id: String,
    size: u64,
    pos: Option<u64>,
    reader: Option<Pin<Box<dyn AsyncRead + Send + 'static>>>,
}

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
            file_streams: Arc::new(Mutex::new(HashMap::new())),
        };
        this.sync().await?;
        Ok(this)
    }

    async fn sync(&mut self) -> anyhow::Result<()> {
        let delta_url = self
            .conn
            .query_row(r"SELECT * FROM sync LIMIT 1", params![], |row| {
                row.get::<_, String>("delta_url")
            })
            .optional()?;
        let is_full_sync = delta_url.is_none();

        // TODO: Expiration handling.
        let (items, delta_url) = self.backend.sync(delta_url.as_deref()).await?;

        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        let current_timestamp = current_timestamp();
        txn.execute(r"DELETE FROM sync", params![])?;
        txn.execute(
            r"INSERT INTO sync (time, delta_url) VALUES (?, ?)",
            params![current_timestamp, delta_url],
        )?;

        if is_full_sync {
            txn.execute(r"DELETE FROM item", params![])?;
        }

        {
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

            for item in items {
                let id = item.id.as_ref().expect("missing id").as_str();

                if item.deleted.is_some() {
                    ensure!(item.root.is_none(), "cannot delete root");
                    delete_stmt.execute(params![id])?;
                    continue;
                }

                let ret = (|| {
                    let fsinfo = item
                        .file_system_info
                        .as_ref()
                        .context("missing fileSystemInfo")?;
                    let parse_time = |field: &str| {
                        let time = fsinfo
                            .get(field)
                            .and_then(|v| v.as_str())
                            .context("missing field")?;
                        humantime::parse_rfc3339(time)
                            .with_context(|| format!("invalid format: {time:?}"))
                    };
                    let created_time = parse_time("createdDateTime")
                        .context("failed to get creation time")?
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_secs();
                    let modified_time = parse_time("lastModifiedDateTime")
                        .context("failed to get modified time")?
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_secs();

                    if item.root.is_some() {
                        insert_root_stmt.execute(named_params! {
                            ":id": id,
                            ":created_time": created_time,
                            ":modified_time": modified_time,
                        })?;
                        return Ok(());
                    }

                    let name = item
                        .name
                        .as_ref()
                        .filter(|name| !name.is_empty())
                        .context("missing name")?;
                    let is_dir = match (item.file.is_some(), item.folder.is_some()) {
                        (true, false) => false,
                        (false, true) => true,
                        _ => bail!("unknown file type"),
                    };
                    let size = if is_dir {
                        0
                    } else {
                        *item.size.as_ref().context("missing size")?
                    };
                    let parent_id = (|| item.parent_reference.as_ref()?.get("id")?.as_str())()
                        .context("missing parent id")?;
                    let changed = insert_child_stmt.execute(named_params! {
                        ":id": id,
                        ":is_directory": is_dir,
                        ":name": name,
                        ":size": size,
                        ":parent_id": parent_id,
                        ":created_time": created_time,
                        ":modified_time": modified_time,
                    })?;
                    ensure!(changed != 0, "missing parent directory");
                    Ok(())
                })();
                if let Err(err) = ret {
                    log::warn!(
                        "Ignoring {} (name={:?}, parent={:?}): {}",
                        id,
                        item.name,
                        item.parent_reference,
                        err,
                    );
                }
            }
        }

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

    pub fn open_file(&self, ino: u64) -> Result<()> {
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

        let mut guard = self.file_streams.lock().unwrap();
        match guard.entry(ino) {
            Entry::Occupied(mut ent) => {
                ent.get_mut().ref_count += 1;
            }
            Entry::Vacant(ent) => {
                ent.insert(FileStream {
                    ref_count: 1,
                    id,
                    size,
                    pos: None,
                    reader: None,
                });
            }
        }

        Ok(())
    }

    pub fn close_file(&self, ino: u64) {
        let mut guard = self.file_streams.lock().unwrap();
        if let Some(stream) = guard.get_mut(&ino) {
            stream.ref_count -= 1;
            if stream.ref_count == 0 {
                guard.remove(&ino);
                log::debug!("drop stream of ino {ino:#x}");
            }
        }
    }

    pub fn read_file(
        &self,
        ino: u64,
        offset: u64,
        mut len: usize,
    ) -> Result<impl Future<Output = Result<Vec<u8>>> + 'static> {
        use futures::future::Either;

        assert_ne!(len, 0, "kernel should not read zero");

        let mut reader = {
            let mut guard = self.file_streams.lock().unwrap();
            let stream = guard.get_mut(&ino).ok_or(Error::NotFound)?;
            if stream.size <= offset {
                return Ok(Either::Left(futures::future::ready(Ok(Vec::new()))));
            }
            // Clamp the length.
            len = usize::try_from(stream.size - offset)
                .unwrap_or(usize::MAX)
                .min(len);
            match stream.pos {
                // Read stream.
                Some(pos) if pos == offset => match stream.reader.take() {
                    Some(reader) => reader,
                    None => {
                        log::warn!("racing read at {offset}");
                        return Err(Error::NonsequentialRead);
                    }
                },
                // New stream.
                None => {
                    stream.pos = Some(offset);
                    // This is cheap before polling.
                    self.backend.download(stream.id.clone(), offset)
                }
                Some(pos) => {
                    log::warn!("non-sequential read at {offset}, but stream is at {pos}");
                    return Err(Error::NonsequentialRead);
                }
            }
        };

        let file_streams = Arc::downgrade(&self.file_streams);
        Ok(Either::Right(async move {
            let mut buf = vec![0u8; len];
            match reader.as_mut().read_exact(&mut buf).await {
                Ok(()) => {
                    if let Some(file_streams) = file_streams.upgrade() {
                        if let Some(stream) = file_streams.lock().unwrap().get_mut(&ino) {
                            assert!(stream.reader.replace(reader).is_none());
                            *stream.pos.as_mut().expect("stream is alive") += buf.len() as u64;
                        }
                    }
                    Ok(buf)
                }
                Err(err) => {
                    if let Some(file_streams) = file_streams.upgrade() {
                        if let Some(stream) = file_streams.lock().unwrap().get_mut(&ino) {
                            // Reset stream.
                            assert!(stream.pos.take().is_some());
                        }
                    }
                    Err(Error::Network(err))
                }
            }
        }))
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("post epoch")
        .as_secs()
}
