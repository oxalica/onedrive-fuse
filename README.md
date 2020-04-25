# onedrive-fuse

[![crates.io](https://img.shields.io/crates/v/onedrive-fuse.svg)](https://crates.io/crates/onedrive-fuse)

**WIP**

Mount your [Microsoft OneDrive][onedrive] storage as [FUSE] filesystem.

[onedrive]: https://products.office.com/en-us/onedrive/online-cloud-storage
[FUSE]: https://github.com/libfuse/libfuse

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
    - [ ] create
    - [x] mkdir
    - [ ] mknod
    - [ ] open
      - [ ] O_WRONLY
      - [ ] O_RDWR
      - [ ] O_TRUNC
      - [ ] O_CREAT
    - [x] rename
    - [x] rmdir
    - [ ] setattr
    - [x] unlink
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
  - [ ] Directory tree cache
  - [ ] Read cache
  - [ ] Write cache

## License

GPL-3.0
