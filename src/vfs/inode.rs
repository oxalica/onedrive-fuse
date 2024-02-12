//! Directory hierarchy and item attributes.
use crate::vfs::error::{Error, Result};
use indexmap::IndexMap;
use onedrive_api::{
    option::{DriveItemPutOption, ObjectOption},
    resource::{DriveItem, DriveItemField},
    ConflictBehavior, FileName, ItemId, ItemLocation, OneDrive, Tag,
};
use reqwest::StatusCode;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Mutex as SyncMutex,
    time::SystemTime,
};

#[derive(Debug, Clone)]
pub struct InodeAttr {
    pub size: u64,
    pub mtime: SystemTime,
    pub crtime: SystemTime,
    pub is_directory: bool,
    // Files have CTag, while directories have not.
    pub c_tag: Option<Tag>,
    // Whether this file is changed locally and waiting for uploading.
    pub dirty: bool,
}

impl InodeAttr {
    pub fn parse_item(item: &DriveItem) -> anyhow::Result<InodeAttr> {
        use anyhow::Context;

        fn parse_time(fs_info: &serde_json::Value, field: &str) -> anyhow::Result<SystemTime> {
            let s = fs_info
                .get(field)
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {}", field))?;
            humantime::parse_rfc3339(s).with_context(|| format!("Invalid time: {:?}", s))
        }

        fn parse_attr(item: &DriveItem) -> anyhow::Result<InodeAttr> {
            let fs_info = item
                .file_system_info
                .as_ref()
                .context("Missing file_system_info")?;
            Ok(InodeAttr {
                size: item.size.context("Missing size")? as u64,
                mtime: parse_time(fs_info, "lastModifiedDateTime")?,
                crtime: parse_time(fs_info, "createdDateTime")?,
                is_directory: item.folder.is_some(),
                c_tag: if item.folder.is_some() {
                    None
                } else {
                    Some(item.c_tag.clone().context("Missing c_tag for file")?)
                },
                dirty: false,
            })
        }

        parse_attr(item).with_context(|| format!("Failed to parse item: {:?}", item))
    }
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub item_id: ItemId,
    pub name: String,
    pub attr: InodeAttr,
}

#[derive(Debug, Deserialize)]
pub struct Config {}

pub struct InodePool {
    tree: SyncMutex<InodeTree>,
}

struct InodeTree {
    // ItemId -> Content, (parent_id, parent_child_idx)
    map: HashMap<ItemId, (Inode, Option<(ItemId, usize)>)>,
}

impl InodeTree {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn get(&self, id: &ItemId) -> Option<&Inode> {
        self.map.get(id).map(|(inode, _)| inode)
    }

    fn get_mut(&mut self, id: &ItemId) -> Option<&mut Inode> {
        self.map.get_mut(id).map(|(inode, _)| inode)
    }

    // Insert a new item, or panic if already exists.
    fn insert_item(&mut self, id: ItemId, attr: InodeAttr) {
        assert!(
            self.map.insert(id, (Inode::new(attr), None)).is_none(),
            "Already exists"
        );
    }

    // Remove an existing item, or panic if not exists.
    fn remove_item(&mut self, id: &ItemId) {
        // Detach itself from parent.
        self.set_parent(id, None);
        let (inode, _) = self.map.remove(id).unwrap();
        // For directory, also detach all children.
        if let Inode::Dir { children, .. } = inode {
            for (_, child_id) in children {
                self.set_parent(&child_id, None);
            }
        }
    }

    // Set parent of an existing item, or panic if source item or parent item or does not exists.
    fn set_parent(&mut self, item_id: &ItemId, new_parent: Option<(ItemId, String)>) {
        // Detach from old parent.
        if let Some((parent_id, child_idx)) =
            self.map.get_mut(item_id).expect("Item not exists").1.take()
        {
            let children = self.get_mut(&parent_id).unwrap().children_mut().unwrap();
            children.swap_remove_index(child_idx);
            if child_idx < children.len() {
                // Previous last child is swapped to a `child_idx`. Maintain parent reference.
                let swapped_child_item_id = children[child_idx].clone();
                let (_, parent) = self.map.get_mut(&swapped_child_item_id).unwrap();
                parent.as_mut().unwrap().1 = child_idx;
            }
        }

        // Set a new parent.
        if let Some((new_parent_id, child_name)) = new_parent {
            let (inode, _) = self.map.get_mut(&new_parent_id).expect("Item not exists");
            let children = inode.children_mut().unwrap();
            let (child_idx, old) = children.insert_full(child_name, item_id.clone());
            assert!(old.is_none(), "Duplicated name");
            assert_eq!(child_idx, children.len() - 1);
            self.map.get_mut(item_id).unwrap().1 = Some((new_parent_id, child_idx));
        }
    }
}

#[derive(Debug)]
enum Inode {
    File {
        attr: InodeAttr,
    },
    Dir {
        attr: InodeAttr,
        children: DirChildren,
    },
}

impl Inode {
    fn new(attr: InodeAttr) -> Self {
        if attr.is_directory {
            Self::Dir {
                attr,
                children: DirChildren::new(),
            }
        } else {
            Self::File { attr }
        }
    }

    fn attr(&self) -> &InodeAttr {
        match self {
            Inode::File { attr } | Inode::Dir { attr, .. } => attr,
        }
    }

    fn set_attr(&mut self, new_attr: InodeAttr) {
        let attr = match self {
            Inode::File { attr } | Inode::Dir { attr, .. } => attr,
        };
        assert_eq!(
            attr.is_directory, new_attr.is_directory,
            "Cannot change between file and directory",
        );
        *attr = new_attr;
    }

    fn children(&self) -> Result<&DirChildren> {
        match self {
            Inode::Dir { children, .. } => Ok(children),
            Inode::File { .. } => Err(Error::NotADirectory),
        }
    }

    fn children_mut(&mut self) -> Result<&mut DirChildren> {
        match self {
            Inode::Dir { children, .. } => Ok(children),
            Inode::File { .. } => Err(Error::NotADirectory),
        }
    }
}

// Child name -> Child item id.
type DirChildren = IndexMap<String, ItemId>;

impl InodePool {
    pub const SYNC_SELECT_FIELDS: &'static [DriveItemField] = &[
        // Basic hierarchy information.
        DriveItemField::id,
        DriveItemField::name,
        DriveItemField::parent_reference,
        DriveItemField::root,
        // Delta.
        DriveItemField::deleted,
        // InodeAttr.
        DriveItemField::size,
        DriveItemField::file,
        DriveItemField::file_system_info,
        DriveItemField::folder,
        DriveItemField::c_tag,
    ];

    pub fn new(_config: Config) -> Self {
        Self {
            tree: SyncMutex::new(InodeTree::new()),
        }
    }

    /// Get attribute of an item.
    pub fn get_attr(&self, item_id: &ItemId) -> Result<InodeAttr> {
        let tree = self.tree.lock().unwrap();
        Ok(tree.get(item_id).ok_or(Error::NotFound)?.attr().clone())
    }

    /// Lookup a child by name of an directory item.
    pub fn lookup(&self, parent_id: &ItemId, child_name: &FileName) -> Result<ItemId> {
        let tree = self.tree.lock().unwrap();
        let children = tree.get(parent_id).ok_or(Error::NotFound)?.children()?;
        children
            .get(child_name.as_str())
            .cloned()
            .ok_or(Error::NotFound)
    }

    /// Read entries of a directory.
    pub fn read_dir(&self, parent_id: &ItemId, offset: u64, count: usize) -> Result<Vec<DirEntry>> {
        let tree = self.tree.lock().unwrap();
        let children = tree.get(parent_id).ok_or(Error::NotFound)?.children()?;

        let mut entries = Vec::with_capacity(count);
        let l = (offset as usize).min(children.len());
        let r = (l + count).min(children.len());
        for i in l..r {
            let (name, child_id) = children.get_index(i).unwrap();
            let child_attr = tree.get(child_id).unwrap().attr();
            entries.push(DirEntry {
                name: name.clone(),
                item_id: child_id.clone(),
                attr: child_attr.clone(),
            });
        }
        Ok(entries)
    }

    pub async fn create_dir(
        &self,
        parent_id: &ItemId,
        name: &FileName,
        onedrive: &OneDrive,
    ) -> Result<(ItemId, InodeAttr)> {
        {
            let tree = self.tree.lock().unwrap();
            let children = tree.get(parent_id).ok_or(Error::NotFound)?.children()?;
            if children.contains_key(name.as_str()) {
                return Err(Error::FileExists);
            }
        }

        let item = onedrive
            .create_folder_with_option(
                ItemLocation::from_id(parent_id),
                name,
                DriveItemPutOption::new().conflict_behavior(ConflictBehavior::Fail),
            )
            .await?;
        let attr = InodeAttr::parse_item(&item).expect("Invalid attrs");
        let id = item.id.expect("Missing id");

        let mut tree = self.tree.lock().unwrap();
        tree.insert_item(id.clone(), attr.clone());
        tree.set_parent(&id, Some((parent_id.clone(), name.as_str().to_owned())));

        Ok((id, attr))
    }

    pub async fn rename(
        &self,
        old_parent_id: &ItemId,
        old_name: &FileName,
        new_parent_id: &ItemId,
        new_name: &FileName,
        onedrive: &OneDrive,
    ) -> Result<Option<ItemId>> {
        let mut replaced_item_id = None;
        let item_id = {
            let tree = self.tree.lock().unwrap();
            let old_children = tree.get(old_parent_id).ok_or(Error::NotFound)?.children()?;
            let new_children = tree.get(new_parent_id).ok_or(Error::NotFound)?.children()?;
            if let Some(id) = new_children.get(new_name.as_str()) {
                replaced_item_id = Some(id.clone());
                let attr = tree.get(id).unwrap().attr();
                if attr.is_directory {
                    return Err(Error::IsADirectory);
                }
                if attr.dirty {
                    return Err(Error::Uploading);
                }
            }
            let item_id = old_children
                .get(old_name.as_str())
                .ok_or(Error::NotFound)?
                .clone();
            if tree.get(&item_id).unwrap().attr().dirty {
                return Err(Error::Uploading);
            }
            item_id
        };

        match onedrive
            .move_with_option(
                ItemLocation::from_id(&item_id),
                ItemLocation::from_id(new_parent_id),
                Some(new_name),
                DriveItemPutOption::new().conflict_behavior(ConflictBehavior::Replace),
            )
            .await
        {
            Ok(_) => {}
            // 400 Bad Request is returned when the destination item is not a directory.
            // `error: { code: "invalidRequest", message: "Bad Argument" }`
            Err(e) if e.status_code() == Some(StatusCode::BAD_REQUEST) => {
                return Err(Error::NotADirectory)
            }
            Err(e) => return Err(e.into()),
        }

        log::debug!(
            "Moved file {:?} from {:?}/{} to {:?}/{}, replaced {:?}",
            item_id,
            old_parent_id,
            old_name.as_str(),
            new_parent_id,
            new_name.as_str(),
            replaced_item_id,
        );

        let mut tree = self.tree.lock().unwrap();
        // Remove the old item first, or name collides.
        if let Some(id) = &replaced_item_id {
            tree.remove_item(id);
        }
        tree.set_parent(
            &item_id,
            Some((new_parent_id.clone(), new_name.as_str().to_owned())),
        );

        Ok(replaced_item_id)
    }

    pub async fn remove(
        &self,
        parent_id: &ItemId,
        name: &FileName,
        directory: bool,
        onedrive: &OneDrive,
    ) -> Result<()> {
        let item_id = {
            let tree = self.tree.lock().unwrap();
            let children = tree.get(parent_id).ok_or(Error::NotFound)?.children()?;
            let item_id = children.get(name.as_str()).ok_or(Error::NotFound)?;
            let inode = tree.get(item_id).unwrap();
            if inode.attr().dirty {
                return Err(Error::Uploading);
            }
            if directory && !inode.children()?.is_empty() {
                return Err(Error::DirectoryNotEmpty);
            }
            if !directory && matches!(inode, Inode::Dir { .. }) {
                return Err(Error::IsADirectory);
            }
            item_id.clone()
        };

        onedrive.delete(ItemLocation::from_id(&item_id)).await?;

        self.tree.lock().unwrap().remove_item(&item_id);
        Ok(())
    }

    /// Update attribute of an item. Return updated attribute.
    pub fn update_attr(
        &self,
        item_id: &ItemId,
        f: impl FnOnce(InodeAttr) -> InodeAttr,
    ) -> InodeAttr {
        let mut tree = self.tree.lock().unwrap();
        let inode = tree.get_mut(item_id).unwrap();
        let old_attr = inode.attr().clone();
        inode.set_attr(f(old_attr));
        inode.attr().clone()
    }

    /// Insert a new item to a directory.
    pub fn insert_item(
        &self,
        parent_id: ItemId,
        child_name: &FileName,
        child_id: ItemId,
        child_attr: InodeAttr,
    ) {
        let mut tree = self.tree.lock().unwrap();
        tree.insert_item(child_id.clone(), child_attr);
        tree.set_parent(&child_id, Some((parent_id, child_name.as_str().to_owned())))
    }

    /// `item_id` should be already checked to be in cache.
    pub async fn set_time(
        &self,
        item_id: &ItemId,
        mtime: SystemTime,
        onedrive: &OneDrive,
    ) -> Result<InodeAttr> {
        let opt = ObjectOption::new().select(Self::SYNC_SELECT_FIELDS);
        let mut patch = DriveItem::default();

        patch.file_system_info = Some(Box::new(serde_json::json!({
            "lastModifiedDateTime": humantime::format_rfc3339_seconds(mtime).to_string(),
        })));
        let item = onedrive
            .update_item_with_option(ItemLocation::from_id(item_id), &patch, opt)
            .await?;
        let attr = InodeAttr::parse_item(&item).expect("Invalid attr");
        log::debug!(
            "Set attribute of {:?}: mtime -> {}",
            item_id,
            humantime::format_rfc3339_seconds(mtime),
        );

        let mut tree = self.tree.lock().unwrap();
        tree.get_mut(item_id).unwrap().set_attr(attr.clone());
        Ok(attr)
    }

    /// Sync item changes from remote. Items not in cache are skipped.
    pub fn sync_items(&self, updated: &[DriveItem]) {
        let mut tree = self.tree.lock().unwrap();

        // > You should only delete a folder locally if it is empty after syncing all the changes.
        // See: https://docs.microsoft.com/en-us/graph/api/driveitem-delta?view=graph-rest-1.0&tabs=http
        let mut dir_marked_deleted = HashSet::new();

        for item in updated {
            if !(item.file.is_some() || item.folder.is_some()) {
                continue;
            }
            let item_id = item.id.as_ref().expect("Missing id");

            // Remove an existing item.
            if item.deleted.is_some() {
                if tree.get(item_id).is_some() {
                    if item.folder.is_some() {
                        log::debug!("Mark remove for directory {:?}", item_id);
                        dir_marked_deleted.insert(item_id);
                    } else {
                        log::debug!("Remove file {:?}", item_id);
                        tree.remove_item(item_id);
                    }
                }
                continue;
            }

            let parent_id = if item.root.is_some() {
                None
            } else {
                let parent_id = (|| {
                    let id = item.parent_reference.as_ref()?.get("id")?.as_str()?;
                    Some(ItemId(id.to_owned()))
                })()
                .expect("Missing new parent for non-root item");

                match tree.get(&parent_id) {
                    // Normal case: parent is a directory.
                    Some(Inode::Dir { .. }) => Some(parent_id),
                    // Some items are children of non-directories. This can happen on `.one` files.
                    // We simply skip them.
                    Some(Inode::File { .. }) => {
                        log::debug!("Skip sub-file item {:?}", item_id);
                        continue;
                    }
                    // FIXME: In some case, there are files linked to unknown parents.
                    // Not sure what's happening here.
                    // https://github.com/oxalica/onedrive-fuse/issues/1
                    None => {
                        log::warn!(
                            "Skip item {:?} with unexpected new parent: {:?}",
                            item_id,
                            item,
                        );
                        continue;
                    }
                }
            };

            match tree.get_mut(item_id) {
                // Insert a new item.
                None => {
                    log::debug!("Insert item {:?}", item_id);
                    let attr = InodeAttr::parse_item(item).expect("Invalid attrs");
                    tree.insert_item(item_id.clone(), attr);
                }
                // Update an existing item.
                Some(inode) => {
                    log::debug!("Update item {:?}", item_id);
                    let attr = InodeAttr::parse_item(item).expect("Invalid attrs");
                    inode.set_attr(attr);
                }
            }

            // Update parent for non-root items.
            if let Some(parent_id) = parent_id {
                let name = item.name.clone().expect("Missing name");
                tree.set_parent(item_id, Some((parent_id, name)));
            }
        }

        // Clean up empty folders which are marked deleted.
        for item_id in dir_marked_deleted {
            if let Some(inode) = tree.get(item_id) {
                if let Ok(children) = inode.children() {
                    if children.is_empty() {
                        log::debug!("Remove directory {:?}", item_id);
                        tree.remove_item(item_id);
                    }
                }
            }
        }
    }
}
