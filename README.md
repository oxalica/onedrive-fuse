# onedrive-fuse

[![crates.io](https://img.shields.io/crates/v/onedrive-fuse.svg)](https://crates.io/crates/onedrive-fuse)

**WIP**

Mount your [Microsoft OneDrive][onedrive] storage as [FUSE] filesystem.

[onedrive]: https://products.office.com/en-us/onedrive/online-cloud-storage
[FUSE]: https://github.com/libfuse/libfuse

## Usage

**TODO**

## Features implemented

- [ ] FUSE syscalls
  - [x] Read
    - [x] access
    - [x] forget
    - [x] getattr
    - [x] lookup
    - [x] open
      - [x] O_RDONLY
    - [x] opendir
    - [x] read
    - [x] readdir
    - [x] release
    - [x] releasedir
    - [x] statfs
  - [ ] Write
    - [x] create
    - [x] mkdir
    - [x] open
      - [x] O_WRONLY/O_RDWR
      - [x] O_TRUNC
      - [x] O_EXCL
    - [x] rename
    - [x] rmdir
    - [ ] setattr
      - [x] Set size
      - [ ] Times
    - [x] unlink
    - [x] write
  - [x] Other
    - destroy
    - flush
    - [x] fsync
    - [x] fsyncdir
    - init
  - Unsupported
    - bmap
    - getlk
    - getxattr
    - link
    - listxattr
    - mknod
    - readlink
    - removexattr
    - setlk
    - setxattr
    - symlink
- [x] Cache
  - [x] Statfs cache
  - [x] Inode attributes (stat) cache
  - [x] Directory tree cache
  - [x] Sync remote changes with local cache
  - [x] File read cache
  - [x] File write cache/buffer

## License

GPL-3.0
