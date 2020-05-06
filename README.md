# onedrive-fuse

[![crates.io](https://img.shields.io/crates/v/onedrive-fuse.svg)](https://crates.io/crates/onedrive-fuse)

**WIP**

Mount your [Microsoft OneDrive][onedrive] storage as [FUSE] filesystem.

[onedrive]: https://products.office.com/en-us/onedrive/online-cloud-storage
[FUSE]: https://github.com/libfuse/libfuse

## Features implemented

- [ ] FUSE syscalls
  - [ ] Read
    - [ ] access
    - [x] forget
    - [x] getattr
    - [x] lookup
    - [ ] open
      - [ ] O_RDONLY
    - [x] opendir
    - [ ] read
    - [x] readdir
    - [ ] release
    - [x] releasedir
    - [x] statfs
  - [ ] Write
    - [ ] create
    - [ ] mkdir
    - [ ] mknod
    - [ ] open
      - [ ] O_WRONLY
      - [ ] O_RDWR
      - [ ] O_TRUNC
      - [ ] O_CREAT
    - [ ] rename
    - [ ] rmdir
    - [ ] setattr
    - [ ] unlink
    - [ ] write
  - [ ] Other
    - destroy
    - [ ] flush
    - [ ] fsync
    - [ ] fsyncdir
    - [ ] getlk
    - init
    - [ ] setlk
  - Unsupported
    - bmap
    - getxattr
    - link
    - listxattr
    - readlink
    - removexattr
    - setxattr
    - symlink
- [ ] Cache
  - [x] Statfs cache
  - [ ] Directory tree cache
  - [ ] Read cache
  - [ ] Write cache

## License

GPL-3.0
