use indexmap::IndexMap;
use slab::Slab;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::SystemTime;

use crate::api::DriveItem;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ino(pub u64);

#[derive(Debug, Clone)]
pub struct Inode {
    parent: Option<(Ino, usize)>,
    pub id: Option<String>,
    pub size: u64,
    pub last_modified_time: SystemTime,
    pub created_time: SystemTime,
    pub data: InodeData,
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

impl InodePool {
    const _ASSERT_ROOT_INO_IS_ONE: [(); 1] = [(); (fuser::FUSE_ROOT_ID == 1) as usize];
    const ROOT_INO: Ino = Ino(fuser::FUSE_ROOT_ID as u64);

    pub fn new(root_item: DriveItem) -> Self {
        let root = Inode {
            parent: None,
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
        let (parent_ino, old_idx) = self.get_mut(ino)?.parent.ok_or(Error::InvalidArgument)?;
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
        let is_dir = item.folder.is_some();

        // Delete item.
        if item.deleted.is_some() {
            if let Some(ino) = ino {
                self.detach(ino)?;
                let inode = self.pool.remove(ino.0 as usize);
                if inode.data.is_dir() {
                    return Err("Recursive deletion".into());
                }
            }
            return Ok(());
        }

        let parent = item.parent_reference.as_ref().ok_or("Missing parent")?;
        match ino {
            // New item.
            None => {
                let parent_ino = match self.id_map.get(&parent.id) {
                    Some(parent) => *parent,
                    // Ignore files under special directories.
                    None => return Ok(()),
                };
                let inode = Inode {
                    parent: None,
                    id: Some(item.id.clone()),
                    size: if is_dir { 0 } else { item.size },
                    last_modified_time: item.last_modified_date_time,
                    created_time: item.created_date_time,
                    data: match is_dir {
                        true => InodeData::Dir(IndexMap::new()),
                        false => InodeData::File,
                    },
                };
                let ino = Ino(self.pool.insert(inode) as u64);
                self.id_map.insert(item.id.clone(), ino);
                let parent_children = self
                    .get_mut(parent_ino)
                    .unwrap()
                    .data
                    .as_dir_mut()
                    .map_err(|_| "Parent is a file")?;
                let ret = parent_children.insert(item.name.clone(), ino);
                if ret.is_some() {
                    return Err("Duplicated children".into());
                }
            }
            // Change item.
            Some(ino) => {
                let inode = self.get_mut(ino).unwrap();
                if !is_dir {
                    inode.size = item.size;
                }
                inode.last_modified_time = item.last_modified_date_time;
                inode.created_time = item.created_date_time;
                if inode.data.is_dir() != item.folder.is_some() {
                    return Err("File type changed".into());
                }

                // Ignore files moved to special directories.
                if let Some(&parent_ino) = self.id_map.get(&parent.id) {
                    self.reparent(ino, parent_ino, item.name.clone())?;
                }
            }
        }
        Ok(())
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
