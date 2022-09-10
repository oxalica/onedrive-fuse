use crate::api::DriveItem;
use crate::error::{Error, Result};
use indexmap::IndexMap;
use slab::Slab;
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::SystemTime;

const MAX_PATH_LEN: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ino(pub u64);

#[derive(Debug, Clone)]
pub struct Inode {
    parent: Option<(Ino, usize)>,
    dirty: Cell<bool>,
    /// The remote id of this inode. For newly created inode, this is None.
    pub id: Option<String>,
    pub size: u64,
    pub last_modified_time: SystemTime,
    pub created_time: SystemTime,
    pub data: InodeData,
}

impl Inode {
    fn update_meta(&mut self, item: &DriveItem) {
        if item.folder.is_none() {
            self.size = item.size.unwrap_or(0);
        }
        self.last_modified_time = item.last_modified_date_time;
        self.created_time = item.created_date_time;
    }
}

#[derive(Debug, Clone)]
pub enum InodeData {
    Dir(IndexMap<String, Ino>),
    File,
}

impl InodeData {
    pub fn is_dir(&self) -> bool {
        matches!(self, Self::Dir(..))
    }

    pub fn as_dir(&self) -> Result<&IndexMap<String, Ino>> {
        match self {
            Self::Dir(children) => Ok(children),
            Self::File => Err(Error::IsNotDirectory),
        }
    }

    pub fn as_dir_mut(&mut self) -> Result<&mut IndexMap<String, Ino>> {
        match self {
            Self::Dir(children) => Ok(children),
            Self::File => Err(Error::IsNotDirectory),
        }
    }

    pub fn get_child(&self, name: &str) -> Result<Ino> {
        self.as_dir()?.get(name).ok_or(Error::NotFound).copied()
    }
}

#[derive(Debug)]
pub struct InodePool {
    pool: Slab<Inode>,
    id_map: HashMap<String, Ino>,
}

#[derive(Debug)]
pub enum InodeChange {
    CreateDir { path: String },
}

impl InodePool {
    const _ASSERT_ROOT_INO_IS_ONE: [(); 1] = [(); (fuser::FUSE_ROOT_ID == 1) as usize];
    const ROOT_INO: Ino = Ino(fuser::FUSE_ROOT_ID as u64);

    pub fn new(root_item: DriveItem) -> Self {
        let root = Inode {
            parent: None,
            dirty: Cell::new(false),
            id: Some(root_item.id.clone()),
            size: 0,
            last_modified_time: root_item.last_modified_date_time,
            created_time: root_item.created_date_time,
            data: InodeData::Dir(IndexMap::new()),
        };

        let mut pool = Slab::with_capacity(4);
        // Reserve inode 0 unused.
        assert_eq!(pool.insert(root.clone()), 0);
        // Root has inode 1.
        assert_eq!(pool.insert(root), Self::ROOT_INO.0 as usize);

        let mut id_map = HashMap::new();
        id_map.insert(root_item.id, Self::ROOT_INO);

        Self { pool, id_map }
    }

    pub fn get(&self, ino: Ino) -> Result<&Inode> {
        self.pool.get(ino.0 as usize).ok_or(Error::NotFound)
    }

    fn get_mut(&mut self, ino: Ino) -> Result<&mut Inode> {
        self.pool.get_mut(ino.0 as usize).ok_or(Error::NotFound)
    }

    fn detach(&mut self, ino: Ino) -> Result<()> {
        // Note that we cannot detach the root.
        let (parent_ino, old_idx) = dbg!(self.get_mut(ino)?)
            .parent
            .ok_or(Error::InvalidArgument)?;
        let parent_children = self
            .get_mut(parent_ino)
            .expect("Tree invariant")
            .data
            .as_dir_mut()
            .expect("Tree invariant");
        parent_children.swap_remove_index(old_idx);
        // Handle relocations.
        if let Some((_, &swapped_ino)) = parent_children.get_index(old_idx) {
            self.get_mut(swapped_ino).expect("Tree invariant").parent = Some((parent_ino, old_idx));
        }
        Ok(())
    }

    fn reparent(&mut self, ino: Ino, new_parent: Ino, new_name: String) -> Result<()> {
        // Validate the target first.
        if let Some(&target_ino) = self.get(new_parent)?.data.as_dir()?.get(&new_name) {
            // No-op
            if target_ino == ino {
                return Ok(());
            }
            return Err(Error::FileExists);
        }

        // Detach from the old children list.
        self.detach(ino)?;

        // Insert to the new children list.
        let new_children = self
            .get_mut(new_parent)
            .expect("Checked early")
            .data
            .as_dir_mut()
            .expect("Checked early");
        let (new_idx, _) = new_children.insert_full(new_name, ino);
        self.get_mut(ino).expect("Checked early").parent = Some((new_parent, new_idx));

        Ok(())
    }

    pub fn apply_change(&mut self, item: &DriveItem) -> Result<()> {
        let ino = self.id_map.get(&item.id).copied();

        // Special case for the root.
        if item.root.is_some() {
            self.get_mut(Self::ROOT_INO)
                .expect("Root exists")
                .update_meta(item);
            return Ok(());
        }

        // We've ruled out the root.
        // Here valid item must has parent, except for deletion notification.
        // We delete items, if they're deleted remotely or moved to an invalid directory.
        let maybe_parent_ino = item
            .parent_reference
            .as_ref()
            .and_then(|parent| self.id_map.get(&parent.id).copied())
            .filter(|&parent_ino| self.get(parent_ino).expect("Tree invariant").data.is_dir());
        let parent_ino = match maybe_parent_ino {
            // Parent is valid, and the item is not deleted.
            Some(parent_ino) if item.deleted.is_none() => parent_ino,
            // Otherwise, remove it from the tree if exists.
            _ => {
                match ino {
                    None => log::debug!("Item ignored: {:?}", item),
                    Some(ino) => {
                        log::debug!("Item deleted: {:?}", item);
                        self.detach(ino).expect("From id map and is not root");
                        let inode = self.pool.remove(ino.0 as usize);
                        if inode.data.is_dir() {
                            return Err("Recursive deletion".into());
                        }
                    }
                }
                return Ok(());
            }
        };

        // Item change.
        if let Some(ino) = ino {
            log::debug!("Item updated: {:?}", item);
            self.get_mut(ino).expect("From id map").update_meta(item);
            self.reparent(ino, parent_ino, item.name.clone())?;
            return Ok(());
        }

        // Item creation.
        let new_ino = Ino(self.pool.vacant_key() as u64);
        let parent_children = self
            .get_mut(parent_ino)
            .unwrap()
            .data
            .as_dir_mut()
            .expect("Checked");

        if let Some(&old_ino) = parent_children.get(&item.name) {
            let inode = self.get_mut(old_ino).expect("Tree invariant");
            inode.update_meta(item);
            match &inode.id {
                None => {
                    log::debug!("Item got id: {:?}", item);
                    inode.id = Some(item.id.clone());
                    self.id_map.insert(item.id.clone(), old_ino);
                    return Ok(());
                }
                Some(_) => {
                    todo!("Item replaced: {:?}", item);
                }
            }
        }

        // Remote new files.
        log::debug!("Item created: {:?}", item);
        let (idx, _) = parent_children.insert_full(item.name.clone(), new_ino);
        let inode = Inode {
            parent: Some((parent_ino, idx)),
            dirty: Cell::new(false),
            id: Some(item.id.clone()),
            size: if item.folder.is_some() {
                0
            } else {
                item.size.unwrap_or(0)
            },
            last_modified_time: item.last_modified_date_time,
            created_time: item.created_date_time,
            data: match item.folder.is_some() {
                true => InodeData::Dir(IndexMap::new()),
                false => InodeData::File,
            },
        };
        assert_eq!(self.pool.insert(inode), new_ino.0 as usize);
        self.id_map.insert(item.id.clone(), new_ino);

        Ok(())
    }

    pub fn take_changes(&mut self) -> Vec<InodeChange> {
        let mut changes = Vec::new();
        let mut path = String::with_capacity(MAX_PATH_LEN);
        self.collect_changes(Self::ROOT_INO, &mut path, &mut changes);
        changes
    }

    // FIXME: This always traverse the whole tree.
    fn collect_changes(&self, ino: Ino, path: &mut String, changes: &mut Vec<InodeChange>) {
        let inode = self.get(ino).expect("Tree invariant");
        if inode.dirty.replace(false) && inode.id.is_none() {
            assert!(!path.is_empty(), "Root is always present");
            changes.push(InodeChange::CreateDir { path: path.clone() })
        }
        let children = match inode.data.as_dir() {
            Ok(children) => children,
            Err(_) => return,
        };
        for (name, &child_ino) in children {
            let prev_path_len = path.len();
            path.push('/');
            path.push_str(name);
            self.collect_changes(child_ino, path, changes);
            path.truncate(prev_path_len);
        }
    }

    pub fn create_dir(&mut self, parent: Ino, name: &str, created_time: SystemTime) -> Result<Ino> {
        let ino = Ino(self.pool.vacant_key() as u64);

        // Check the destination.
        let children = self.get_mut(parent)?.data.as_dir_mut()?;
        if children.contains_key(name) {
            return Err(Error::FileExists);
        }

        // Insert children and allocate inode.
        let (idx, _) = children.insert_full(name.into(), ino);
        let inode = Inode {
            parent: Some((parent, idx)),
            dirty: Cell::new(true),
            id: None,
            size: 0,
            last_modified_time: created_time,
            created_time,
            data: InodeData::Dir(IndexMap::new()),
        };
        assert_eq!(self.pool.insert(inode), ino.0 as usize);

        Ok(ino)
    }
}

// Ref: https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions
// But also with some experiments.
pub fn check_file_name(name: &OsStr) -> Result<&str> {
    fn check(s: &str) -> bool {
        !s.bytes()
            .any(|b| b"<>:\"/\\|?*".contains(&b) || (0..=31).contains(&b))
            && !s.ends_with('.')
            && !s.ends_with(' ')
    }
    name.to_str()
        .filter(|s| check(s))
        .ok_or(Error::InvalidArgument)
}
